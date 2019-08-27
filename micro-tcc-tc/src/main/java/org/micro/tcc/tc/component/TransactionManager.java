package org.micro.tcc.tc.component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.*;
import org.micro.tcc.common.exception.CancelException;
import org.micro.tcc.common.exception.ConfirmException;
import org.micro.tcc.common.exception.NoExistedTransactionException;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.tc.repository.RedisSpringTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

/**
*@author jeff.liu
*@desc   事务管理器
 *分布式事务统一管理，主要和协调者交互
*@date 2019/8/23
*/
@Slf4j
public class TransactionManager {


    /**
     * 事务日志保存
     */
    private TransactionRepository transactionRepository;

    private Cache<String, Transaction> TRANSACTION_CACHE = CacheBuilder.newBuilder().maximumSize(500000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    /**
     * 当前线程
     */
    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    /**
     * 分布式事务线程组
     */
    private ExecutorService executorService;
    private ExecutorService futureEcutorService;

    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }
    public static TransactionManager transactionManager=new TransactionManager();

    public static TransactionManager getInstance(){
        return transactionManager;
    }

    public  TransactionManager(){

       if(transactionRepository==null){
            transactionRepository= RedisSpringTransactionRepository.getInstance();
        }

        if (executorService == null || futureEcutorService==null) {

            synchronized (TransactionManager.class) {

                if (executorService == null) {
                    executorService = Executors.newCachedThreadPool();
                }
                if (futureEcutorService == null) {
                    futureEcutorService = Executors.newCachedThreadPool();
                }
            }
        }

    }


    public ExecutorService getExecutorService()
    {
        return executorService;
    }

    public String getTransactionGlobalId(){
        Transaction transaction=getCurrentTransaction();
        String gid="";
        if(null!=transaction){
            gid=transaction.getTransactionXid().getGlobalTccTransactionId();
        }
        return gid;
    }

    public Transaction begin(Object uniqueIdentify) {
        Transaction transaction = new Transaction(uniqueIdentify, TransactionType.ROOT);
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }

    /**
     * 开启事务
     * @return
     * @throws Exception
     */
    public Transaction begin() throws Exception{
        Transaction transaction = new Transaction(TransactionType.ROOT);
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }



    public Transaction propagationSupportsStart(TccTransactionContext transactionContext) {
        Transaction transaction = new Transaction(transactionContext);
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }

    /**
     * 判断是否存在事务
     * @param transactionContext
     * @return
     * @throws NoExistedTransactionException
     */
    public Transaction propagationExistStart(TccTransactionContext transactionContext) throws NoExistedTransactionException {
        Transaction transaction = transactionRepository.findByGroupId(transactionContext.getXid().getGlobalTccTransactionId());
        if (transaction != null) {
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            registerTransaction(transaction);
            return transaction;
        } else {
            throw new NoExistedTransactionException("TCC:Group id 不存在！"+transactionContext.getXid().getGlobalTccTransactionId());
        }
    }
    public boolean isExitGlobalTransaction(String groupId){
       Transaction transaction= peekCache(groupId);
       if(null!=transaction){
           return true;
       }
       return false;
    }
    public void putToCache(String key,Transaction transaction){
        TRANSACTION_CACHE.put(key,transaction);
    }
    public void putToCache(Transaction transaction){
        TRANSACTION_CACHE.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }
    public Transaction peekCache(String key){
        return TRANSACTION_CACHE.getIfPresent(key);
    }

    public void delCache(String key){
        TRANSACTION_CACHE.invalidate(key);
    }

    /**
     * 异步处理事务
     * @param groupId
     * @param status
     */
    public void syncProcess(final String groupId,final String status){
        Transaction transaction = null;
        try {
            if(!isExitGlobalTransaction(groupId)){
                log.debug("TCC:服务器不存在globalTransactionId:{}",groupId);
                return;
            }
            int _status = Integer.parseInt(status);
            TransactionXid transactionXid = new TransactionXid(groupId);
            TccTransactionContext transactionContext = new TccTransactionContext(transactionXid, _status);
            switch (TransactionStatus.valueOf(_status)) {
                case TRY:
                    break;
                case CONFIRM:
                    try {
                        transaction = propagationExistStart(transactionContext);
                        boolean asyncConfirm = false;
                        if ("true".equals(transaction.getAsyncConfirm())) {
                            asyncConfirm = true;
                        }
                        commit(transaction, asyncConfirm);
                        cleanAfterConfirm(transaction);
                    } catch (Throwable excepton) {
                        log.error(excepton.getMessage(),excepton);
                        //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                        if(null==transaction){
                            break;
                        }

                        this.sendCancelOrderToMember(transaction);

                    }finally{

                    }
                    break;
                case CANCEL:
                    try {
                        transaction = propagationExistStart(transactionContext);
                        boolean asyncCancel = false;
                        if ("true".equals(transaction.getAsyncConfirm())) {
                            asyncCancel = true;
                        }
                        rollback(transaction, asyncCancel);
                        cleanAfterCancel(transaction);
                    } catch (NoExistedTransactionException exception) {

                    }finally{


                    }
                    break;
                default:
                    break;
            }

        }catch (Throwable t){
            log.error("TCC:分布式事务管理器发生错误：",t);
        }
    }

    /**
     * 处理zk 事件
     * @param groupId
     * @param status
     */
    public void process(String groupId,String status){
        if(StringUtils.isEmpty(groupId)){
            return;
        }
        Future future= executorService.submit(new Runnable() {
            @Override
            public void run() {
                syncProcess(groupId,status);
            }
        });
        /*try {
            futureEcutorService.submit(new Runnable() {
                @Override
                public void run() {
                    CoordinatorWatcher.getInstance().processTransactionStart(future);
                }
            });

        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }*/


    }

    /**
     * 回滚事务by客户端
     * @throws Exception
     */
    public void rollbackForClient() throws Exception {
       Transaction transaction =getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CANCEL);
        CoordinatorWatcher.getInstance().modify(transaction);
    }

    /**
     * 提交事务
     * @param transactionArg
     * @param asyncCommit
     */
    public void commit(Transaction transactionArg,boolean asyncCommit) {
        final Transaction transaction;
        if(null==transactionArg){
            transaction = getCurrentTransaction();
        }else {
            transaction = transactionArg;
        }
        /*transaction.changeStatus(TransactionStatus.CONFIRM);
        transactionRepository.update(transaction);*/
        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });

            } catch (Throwable commitException) {
                throw new ConfirmException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }


    /**
     * 回滚事务
     * @param transactionArg
     * @param asyncRollback
     */
    public void rollback(Transaction transactionArg,boolean asyncRollback) {
        final Transaction transaction;
        if(null==transactionArg){
            transaction = getCurrentTransaction();
        }else {
            transaction = transactionArg;
        }
        transaction.changeStatus(TransactionStatus.CANCEL);
        transactionRepository.update(transaction);
        if (asyncRollback) {
            try {
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        rollbackTransaction(transaction);
                    }
                });
            } catch (Throwable rollbackException) {
                log.warn("TCC:transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancelException(rollbackException);
            }
        } else {
            rollbackTransaction(transaction);
        }
    }

    /**
     * 提交事务
     * @param transaction
     */
    private void commitTransaction(Transaction transaction) {
        try {
            transaction.commit();
            transactionRepository.delete(transaction);
            switch (TransactionType.valueOf(transaction.getTransactionType().value())){
                case ROOT:
                    CoordinatorWatcher.getInstance().deleteDataNodeForConfirm(transaction);
                    break;
                default:
                    break;
            }
        } catch (Throwable commitException) {
            log.warn("TCC: transaction confirm failed.", commitException);
            throw new ConfirmException(commitException);
        }
    }

    /**
     * 回滚事务
     * @param transaction
     */
    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback();
            transactionRepository.delete(transaction);
            switch (TransactionType.valueOf(transaction.getTransactionType().value())){
                case ROOT:
                    CoordinatorWatcher.getInstance().deleteDataNodeForCancel(transaction);
                    break;
                default:
                    break;
            }
        } catch (Throwable rollbackException) {
            log.error("TCC: transaction rollback failed.", rollbackException);
            //throw new CancelException(rollbackException);
        }
    }

    /**
     * 获取当前事务
     * @return
     */
    public Transaction getCurrentTransaction() {
        if(CURRENT.get()!=null){
            return CURRENT.get().peek();
        }
       return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 注册事务
     * @param transaction
     */
    private void registerTransaction(Transaction transaction) {

        if (CURRENT.get() == null) {
            CURRENT.set(new LinkedList<Transaction>());
        }
        CURRENT.get().push(transaction);

    }

    public void registerTransactionTrace(Transaction transaction) {
        registerTransaction(transaction);
    }

    public void cleanAfterCompletion(Transaction transaction) {
        //clean(transaction);

    }

    public void cleanAfterConfirm(Transaction transaction) {
        clean(transaction);
        delCache(transaction.getTransactionXid().getGlobalTccTransactionId());
        try {
            //CoordinatorWatcher.getInstance().deleteDataNodeForConfirm(transaction);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

    }

    /**
     * 清楚缓存
     * @param transaction
     */
    private void clean(Transaction transaction){
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                CURRENT.get().pop();
                if (CURRENT.get().size() == 0) {
                    CURRENT.remove();
                }
            } else {
                throw new TccSystemErrorException("本地线程变量异常！");
            }
        }
    }

    /**
     * 删除缓存
     * @param transaction
     */
    public void cleanAfterCancel(Transaction transaction) {
        clean(transaction);
        delCache(transaction.getTransactionXid().getGlobalTccTransactionId());
        try {
            //CoordinatorWatcher.getInstance().deleteDataNode(transaction);
        } catch (Exception e) {
            log.error(e.getMessage(),e);
        }

    }

    /**
     * 添加参与者
     * @param participant
     */
    public void addingParticipants(TransactionMember participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.addingParticipants(participant);
        transactionRepository.update(transaction);
    }

    public void saveConfirmOrder(Transaction transaction) throws Exception{
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                transaction.changeStatus(TransactionStatus.CONFIRM);
                transactionRepository.update(transaction);
            }
        });
    }
    public void sendConfirmOrderToMember(Transaction transaction) throws Exception{
        transaction.changeStatus(TransactionStatus.CONFIRM);
        CoordinatorWatcher.getInstance().modify(transaction);

    }

    public void sendCancelOrderToMember(Transaction transaction) throws Exception{
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                transactionRepository.createGroupMemberForCancel(transaction);
            }
        });
        transaction.changeStatus(TransactionStatus.CANCEL);
        CoordinatorWatcher.getInstance().modify(transaction);

    }

    /**
     * 添加参与者
     * @param transaction
     * @throws Exception
     */
    public void addGroupForMember(Transaction transaction) throws Exception{
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                transactionRepository.createGroupMember(transaction);
            }
        });
        //CoordinatorWatcher.getInstance().add(transaction);
    }
}

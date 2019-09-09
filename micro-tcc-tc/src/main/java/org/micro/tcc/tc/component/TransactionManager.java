package org.micro.tcc.tc.component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.common.constant.Model;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.*;
import org.micro.tcc.common.exception.CancelException;
import org.micro.tcc.common.exception.ConfirmException;
import org.micro.tcc.common.exception.NoExistedTransactionException;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.tc.interceptor.TccMethodContext;
import org.micro.tcc.tc.repository.RedisSpringTransactionRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

/**
*@author jeff.liu
*@desc   事务管理器
 *分布式事务统一管理，主要和协调者交互
*@date 2019/8/23
*/
@Slf4j
@Component("DistributeTransactionManager")
public class TransactionManager {


    /**
     * 事务日志保存
     */
    private TransactionRepository transactionRepository;

    private volatile Cache<String, Transaction> transactionCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    private volatile Cache<String, Transaction> cancelTransactionCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(30L, TimeUnit.MINUTES).build();


    /**
     * 当前线程
     */
    private static final ThreadLocal<Deque<Transaction>> currentThreadLoacl = new ThreadLocal<Deque<Transaction>>();

    /**
     * 分布式事务线程组
     */
    private ExecutorService executorService;



    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public static TransactionManager getInstance(){
        TransactionManager transactionManager= SpringContextAware.getBean(TransactionManager.class);
        return transactionManager;
    }

    public TransactionManager(@Value("${micro.tcc.transaction.executorService.corePoolSize:10}") int corePoolSize,
                              @Value("${micro.tcc.transaction.executorService.maximumPoolSize:30}") int maximumPoolSize,
                              @Value("${micro.tcc.transaction.executorService.keepAliveTime:60}") long keepAliveTime
                                        ){

       if(transactionRepository==null){
            transactionRepository= RedisSpringTransactionRepository.getInstance();
        }

        if (executorService == null ) {
            synchronized (TransactionManager.class) {
                if (executorService == null) {
                    ThreadFactory threadFactory= new ThreadFactoryBuilder().setNameFormat("Tcc-TM-Pool-%d").build();
                    executorService =  new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),threadFactory);
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
    public Transaction begin(TccMethodContext tccMethodContext) throws Exception{
        Transaction transaction = new Transaction(TransactionType.ROOT);
        return register(tccMethodContext,transaction);
    }

    public Transaction register(TccMethodContext tccMethodContext,Transaction transaction ){
        boolean asyncConfirm = tccMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = tccMethodContext.getAnnotation().asyncCancel();
        int model=tccMethodContext.getAnnotation().model().value();
        transaction.setModel(model);
        transaction.setAsyncCancel(asyncCancel);
        transaction.setAsyncConfirm(asyncConfirm);
        transaction.setRollbackFor(tccMethodContext.getAnnotation().rollbackFor().getClass());
        transactionRepository.create(transaction);
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }

    public Transaction propagationSupportsBegin(TccMethodContext tccMethodContext) {
        Transaction transaction = new Transaction(tccMethodContext.getTransactionContext());
        return register(tccMethodContext,transaction);
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
            putToCache(transaction);
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
        transactionCache.put(key,transaction);
    }
    public void putToCache(Transaction transaction){
        transactionCache.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }
    public Transaction peekCache(String key){
        return transactionCache.getIfPresent(key);
    }

    public void delCache(String key){
        transactionCache.invalidate(key);
    }


    public void putToCancelCache(Transaction transaction){
        this.cancelTransactionCache.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }
    public Transaction peekCancelCache(String key){
        return cancelTransactionCache.getIfPresent(key);
    }
    public Transaction peekCancelCache(Transaction transaction){
        return cancelTransactionCache.getIfPresent(transaction.getTransactionXid().getGlobalTccTransactionId());
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
                        if (transaction.getAsyncConfirm()) {
                            asyncConfirm = true;
                        }
                        commit(transaction, asyncConfirm);
                        cleanAfterConfirm(transaction);
                    } catch (Throwable exception) {
                        log.error(exception.getMessage(),exception);
                        //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                        if(null==transaction){
                            break;
                        }
                        //AT模式，会自动回滚事务
                        if(transaction.getModel()== Model.AT.value()){
                            Class rollbackFor=transaction.getRollbackFor();
                            //抛出的异常继承rollbackFor
                            if(rollbackFor.isAssignableFrom(exception.getClass())){
                                this.sendAndExecuteCancelOrderToMember(transaction);
                            }
                        }
                    }finally{

                    }
                    break;
                case CANCEL:
                    try {
                        transaction = propagationExistStart(transactionContext);
                        boolean asyncCancel = false;
                        if (transaction.getAsyncConfirm()) {
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
     * 处理协调者 事件
     * @param groupId
     * @param status
     */
    public void process(String groupId,String status){
        if(StringUtils.isEmpty(groupId) || StringUtils.isEmpty(status)){
            log.error("TCC:group id is null or status is null{},{}",groupId,status);
            return;
        }
        int _status = Integer.parseInt(status);
        TransactionStatus transactionStatus=null;
        switch (TransactionStatus.valueOf(_status)) {
            case TRY:
                break;
            case CONFIRM:
                transactionStatus=TransactionStatus.CONFIRM;
                break;
            case CANCEL:
                transactionStatus=TransactionStatus.CANCEL;
                break;
            default:
                break;
        }
        TransactionXid xid=new TransactionXid(groupId);
        Transaction transaction=new Transaction(xid,transactionStatus);
        boolean isExecute=judgeIdempotent(transaction);
        if(isExecute){
            log.debug("TCC:group id {} 已经执行过，为了保证幂等，不能重复执行。",groupId);
            return;
        }
        Future future= executorService.submit(new Runnable() {
            @Override
            public void run() {
                syncProcess(groupId,status);
            }
        });

    }

    /**
     * 回滚事务by客户端
     * @throws Exception
     */
    public void rollbackForClient(String groupId) throws Exception {
        Transaction transaction =peekCache(groupId);
        sendCancelOrderToMember(transaction);
    }
    /**
     * 回滚事务by客户端
     * @throws Exception
     */
    public void rollbackForClient() throws Exception {
        Transaction transaction =getCurrentTransaction();
        sendCancelOrderToMember(transaction);
    }

    private boolean judgeIdempotent(Transaction transaction){
        return transactionRepository.judgeIdempotent(transaction);
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
        if(currentThreadLoacl.get()!=null){
            return currentThreadLoacl.get().peek();
        }
       return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = currentThreadLoacl.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 注册事务
     * @param transaction
     */
    private void registerTransaction(Transaction transaction) {
        if (currentThreadLoacl.get() == null) {
            currentThreadLoacl.set(new LinkedList<Transaction>());
        }
        currentThreadLoacl.get().push(transaction);
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
    }

    /**
     * 清楚缓存
     * @param transaction
     */
    private void clean(Transaction transaction){
        if (isTransactionActive() && transaction != null) {
            Transaction currentTransaction = getCurrentTransaction();
            if (currentTransaction == transaction) {
                currentThreadLoacl.get().pop();
                if (currentThreadLoacl.get().size() == 0) {
                    currentThreadLoacl.remove();
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
        transaction.changeStatus(TransactionStatus.CONFIRM);
        transactionRepository.update(transaction);
    }
    public void sendConfirmOrderToMember(Transaction transaction) throws Exception{
        //如果主调用方已经取消，则向协调者发出取消指令
        if(peekCancelCache(transaction)!=null){
            saveConfirmOrder(transaction);
            executeCancelOrderToMember(transaction);
            return;
        }
        //如果给子系统取消，则向协调者发出取消指令，否则执行Confirm
        if(!transactionRepository.isCanceling(transaction)){
            saveConfirmOrder(transaction);
            transaction.changeStatus(TransactionStatus.CONFIRM);
            CoordinatorWatcher.getInstance().modify(transaction);
        }else {
            executeCancelOrderToMember(transaction);
        }

    }

    public void sendCancelOrderToMember(Transaction transaction) throws Exception{
        transactionRepository.createGroupMemberForCancel(transaction);
    }

    public void executeCancelOrderToMember(Transaction transaction) throws Exception{
        transaction.changeStatus(TransactionStatus.CANCEL);
        CoordinatorWatcher.getInstance().modify(transaction);
    }
    public void sendAndExecuteCancelOrderToMember(Transaction transaction) throws Exception{
        sendCancelOrderToMember(transaction);
        executeCancelOrderToMember(transaction);
    }



}

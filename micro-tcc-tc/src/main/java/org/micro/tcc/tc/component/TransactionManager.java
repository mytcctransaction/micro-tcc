package org.micro.tcc.tc.component;

import com.alibaba.dubbo.rpc.RpcException;
import com.alibaba.fastjson.JSON;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.common.constant.Constant;
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

    /**
     * 当前执行的事务缓存
     */
    private volatile Cache<String, Transaction> transactionCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    /**
     *已提交的事务缓存
     */
    private volatile Cache<String, Transaction> confirmTransactionCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    /**
     *已取消的事务缓存
     */
    private volatile Cache<String, Transaction> cancelTransactionCache = CacheBuilder.newBuilder().maximumSize(100000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    /**
     * 当前线程
     */
    private static final ThreadLocal<Deque<Transaction>> currentThreadLocal = new ThreadLocal<Deque<Transaction>>();

    /**
     * 提交方法和取消方法极端情况下会并发执行，这里用ConcurrentHashMap根据group id 判断锁
     */
    private ConcurrentMap<String,Transaction> locks=new ConcurrentHashMap<String,Transaction>();

    /**
     * 在没有获取锁的情况下，把任务按定期执行
     */
    private static final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * 任务缓存
     */
    private static volatile Map<String, ScheduledFuture> delayTasks = new ConcurrentHashMap<>();
    /**
     * 分布式事务线程组
     */
    private ExecutorService executorService;

    private long lockTime=1000L;



    public void setTransactionRepository(TransactionRepository transactionRepository) {
        this.transactionRepository = transactionRepository;
    }

    public static TransactionManager getInstance(){
        TransactionManager transactionManager= SpringContextAware.getBean(TransactionManager.class);
        return transactionManager;
    }

    public TransactionManager(@Value("${micro.tcc.transaction.executorService.corePoolSize:0}") int corePoolSize,
                              @Value("${micro.tcc.transaction.executorService.maximumPoolSize:0}") int maximumPoolSize,
                              @Value("${micro.tcc.transaction.executorService.keepAliveTime:60}") long keepAliveTime
                                        ){

       if(transactionRepository==null){
            transactionRepository= RedisSpringTransactionRepository.getInstance();
        }

        if (executorService == null ) {
            synchronized (TransactionManager.class) {
                if (executorService == null) {
                    if(corePoolSize==0){
                        corePoolSize=Runtime.getRuntime().availableProcessors();
                        maximumPoolSize=corePoolSize*2;
                    }
                    ThreadFactory threadFactory= new ThreadFactoryBuilder().setNameFormat("Tcc-TM-Pool-%d").build();
                    executorService =  new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),threadFactory);
                    // 等待线程池任务完成
                    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                        executorService.shutdown();
                        try {
                            executorService.awaitTermination(6, TimeUnit.SECONDS);
                        } catch (InterruptedException ignored) {
                        }
                    }));
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
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }

    /**
     * 创建try事务组
     * @param transaction
     */
    public void createGroupMember(Transaction transaction){
        transactionRepository.createGroupMember(transaction);
    }

    /**
     * 创建提交事务组
     * @param transaction
     */
    public void createGroupMemberForConfirm(Transaction transaction){
        transaction.changeStatus(TransactionStatus.CONFIRM);
        transactionRepository.createGroupMember(transaction);
    }
    /**
     * 创建取消事务组
     * @param transaction
     */
    public void createGroupMemberForCancel(Transaction transaction){
        transaction.changeStatus(TransactionStatus.CANCEL);
        transactionRepository.createGroupMember(transaction);
    }

    /**
     * 处理开启新事务
     * @return
     * @throws Exception
     */
    public Transaction propagationRequiredBegin(TccMethodContext tccMethodContext) throws Exception{
        Transaction transaction = new Transaction(TransactionType.ROOT);
        return register(tccMethodContext,transaction);
    }

    /**
     * 注册事务
     * @param tccMethodContext
     * @param transaction
     * @return
     */
    public Transaction register(TccMethodContext tccMethodContext,Transaction transaction ){
        boolean asyncConfirm = tccMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = tccMethodContext.getAnnotation().asyncCancel();
        int model=tccMethodContext.getAnnotation().model().value();
        transaction.setModel(model);
        transaction.setAsyncCancel(asyncCancel);
        transaction.setAsyncConfirm(asyncConfirm);
        transaction.setRollbackFor(tccMethodContext.getAnnotation().rollbackFor());
        createGroupMember(transaction);
        registerTransaction(transaction);
        putToCache(transaction);
        return transaction;
    }

    /**
     * 处理分支事务
     * @param tccMethodContext
     * @return
     */
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

    /**
     * 判断本机缓存是否全局事务
     * @param groupId
     * @return
     */
    public boolean isExitGlobalTransaction(String groupId){
       Transaction transaction= peekCache(groupId);
       if(null!=transaction){
           return true;
       }
       return false;
    }

    /**
     * 加入事务缓存
     * @param key
     * @param transaction
     */
    public void putToCache(String key,Transaction transaction){
        transactionCache.put(key,transaction);
    }

    /**
     * 加入事务缓存
     * @param transaction
     */
    public void putToCache(Transaction transaction){
        transactionCache.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }

    /**
     * 取出事务缓存
     * @param key
     * @return
     */
    public Transaction peekCache(String key){
        return transactionCache.getIfPresent(key);
    }

    /**
     * 删除事务缓存
     * @param key
     */
    public void delCache(String key){
        transactionCache.invalidate(key);
    }

    /**
     * 加入已执行事务缓存
     * @param transaction
     */
    public void putToConfirmTransactionCache(Transaction transaction){
        this.confirmTransactionCache.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }

    /**
     * 取出已执行事务缓存
     * @param key
     * @return
     */
    public Transaction peekConfirmTransactionCache(String key){
        return confirmTransactionCache.getIfPresent(key);
    }

    /**
     * 取出已经执行的事务缓存
     * @param transaction
     * @return
     */
    public Transaction peekConfirmTransactionCache(Transaction transaction){
        return confirmTransactionCache.getIfPresent(transaction.getTransactionXid().getGlobalTccTransactionId());
    }

    /**
     * 加入已取消事务缓存
     * @param transaction
     */
    public void putToCancelTransactionCache(Transaction transaction){
        this.cancelTransactionCache.put(transaction.getTransactionXid().getGlobalTccTransactionId(),transaction);
    }

    /**
     * 取出已执行事务缓存
     * @param key
     * @return
     */
    public Transaction peekCancelTransactionCache(String key){
        return cancelTransactionCache.getIfPresent(key);
    }

    /**
     * 取出已经执行的事务缓存
     * @param transaction
     * @return
     */
    public Transaction peekCancelTransactionCache(Transaction transaction){
        return cancelTransactionCache.getIfPresent(transaction.getTransactionXid().getGlobalTccTransactionId());
    }

    /**
     * 提交事务，并清除缓存
     * @param transaction
     */
    private void commitAndCleanAfterConfirm(Transaction transaction){
        commit(transaction);
        cleanAfterConfirm(transaction);
    }

    /**
     * 回滚事务，并清除缓存
     * @param transaction
     */
    private void rollbackAndCleanAfterCancel(Transaction transaction){
        rollback(transaction);
        cleanAfterCancel(transaction);
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
                    Transaction transactionExist=null;
                    try {
                        transaction = propagationExistStart(transactionContext);
                        //解决多次并发调用问题
                        transactionExist=this.locks.putIfAbsent(groupId,transaction);
                        //如果没有锁，则执行提交,否则不做处理
                        if(null==transactionExist){
                            commitAndCleanAfterConfirm(transaction);
                        }else {
                            log.debug("TCC:有锁，不做处理,groupId:{}",transaction.getTransactionXid().getGlobalTccTransactionId());
                        }
                    } catch(NoExistedTransactionException noExistedTransactionException){
                        log.error("TCC:找不到globalTransactionId:{}",groupId);
                        return;
                    } catch (Throwable exception) {
                        log.error(exception.getMessage(),exception);
                        if(null==transaction){
                            break;
                        }
                        //AT模式，会自动回滚事务
                        if(transaction.getModel()== Model.AT.value()){
                            Class<? extends Throwable>[] rollbackFor=transaction.getRollbackFor();
                            for(Class c:rollbackFor){
                                //如果抛出的异常继承rollbackFor
                                if(c.isAssignableFrom(exception.getClass())){
                                    //创建cancel事务组
                                    createGroupMemberForCancel(transaction);
                                    //创建事务并发送取消操作到每个事务组成员
                                    this.createAndSendCancelOrderToMember(transaction);
                                    break;
                                }
                            }
                        }
                    }finally {
                        if(null==transactionExist){
                            locks.remove(groupId,transaction);
                        }
                    }
                    break;
                case CANCEL:
                    Transaction transactionExistCancel=null;
                    try {
                        final Transaction transactionCancel=propagationExistStart(transactionContext);
                        //查看是否有锁
                        transactionExistCancel=this.locks.putIfAbsent(groupId,transactionCancel);
                        if(null!=transactionExistCancel){
                            switch (TransactionStatus.valueOf(transactionExistCancel.getStatus().value())){
                                case CANCEL:
                                    break;
                                case CONFIRM:
                                    //如果有锁，则发起定时schedule job，重试执行
                                    ScheduledFuture scheduledFuture=scheduledExecutorService.scheduleAtFixedRate(() -> {
                                        Transaction transactionConfirmExist=this.locks.putIfAbsent(groupId,transactionCancel);
                                        log.debug("并发后定时任务执行{}",Thread.currentThread().getName());
                                        if(null==transactionConfirmExist && this.peekCancelTransactionCache(transactionCancel)==null){
                                            rollbackAndCleanAfterCancel(transactionCancel);
                                            log.debug("并发后定时任务等待提交操作完成，执行取消操作{}", JSON.toJSON(transactionCancel));
                                            ScheduledFuture scheduledFutureStop=delayTasks.get(transactionCancel.getTransactionXid().getGlobalTccTransactionId());
                                            if(scheduledFutureStop!=null){
                                                scheduledFutureStop.cancel(true);
                                                delayTasks.remove(transactionCancel.getTransactionXid().getGlobalTccTransactionId());
                                            }
                                        }
                                        if(null==transactionConfirmExist){
                                            locks.remove(groupId,transactionCancel);
                                        }
                                    }, Constant.DELAY,Constant.PERIOD, TimeUnit.MILLISECONDS);
                                    delayTasks.put(transactionCancel.getTransactionXid().getGlobalTccTransactionId(),scheduledFuture);
                                    break;
                                default:
                                    break;
                            }
                        }else {
                            rollbackAndCleanAfterCancel(transactionCancel);
                        }
                    } catch(NoExistedTransactionException noExistedTransactionException){
                        log.error("TCC:找不到globalTransactionId:{}",groupId);
                        return;
                    } catch (Exception exception) {
                    }finally {
                        if(transactionExistCancel==null){
                            locks.remove(groupId);
                        }
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
     * 处理协调者发送过来的事件
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
                //本地执行事务缓存已经存在，则不会重复执行
                if(this.peekConfirmTransactionCache(groupId)!=null){
                    log.debug("TCC:group id {} 已经执行过，为了保证幂等，不能重复执行。",groupId);
                    return;
                }
                break;
            case CANCEL:
                transactionStatus=TransactionStatus.CANCEL;
                //本地执行事务缓存已经存在，则不会重复执行
                if(this.peekCancelTransactionCache(groupId)!=null){
                    log.debug("TCC:group id {} 已经执行过，为了保证幂等，不能重复执行。",groupId);
                    return;
                }
                break;
            default:
                break;
        }
        if(transactionStatus==null){
            log.error("TCC:事务状态不正确！group id:{} ",groupId);
            return;
        }
        if(Constant.IDEMPOTENT.equals(Constant.IDEMPOTENT_TRUE)){
            TransactionXid xid=new TransactionXid(groupId);
            Transaction transaction=new Transaction(xid,transactionStatus);
            //判断redis 执行事务如果已经存在，则不会重复执行
            boolean isExecuted=judgeIdempotent(transaction);
            if(isExecuted){
                log.debug("TCC:group id {} 已经执行过，为了保证幂等，不能重复执行。",groupId);
                return;
            }
        }
        syncProcess(groupId,status);
    }

    /**
     * 回滚事务by客户端
     * @throws Exception
     */
    public void rollbackForClient(String groupId) throws Exception {
        Transaction transaction =peekCache(groupId);
        createGroupMemberForCancel(transaction);
        sendCancelOrderToMember(transaction);
    }
    /**
     * 回滚事务by客户端
     * @throws Exception
     */
    public void rollbackForClient() throws Exception {
        Transaction transaction =getCurrentTransaction();
        createGroupMemberForCancel(transaction);
        sendCancelOrderToMember(transaction);
    }

    /**
     * 判断是否重复执行
     */
    private boolean judgeIdempotent(Transaction transaction){
        return transactionRepository.judgeIdempotent(transaction);
    }

    /**
     * 提交事务
     * @param transactionArg
     */
    public void commit(Transaction transactionArg) {
        final Transaction transaction;
        if(null==transactionArg){
            transaction = getCurrentTransaction();
        }else {
            transaction = transactionArg;
        }
        transaction.changeStatus(TransactionStatus.CONFIRM);
        this.updateTransaction(transaction);
        commitTransaction(transaction);
    }


    /**
     * 回滚事务
     * @param transactionArg
     */
    public void rollback(Transaction transactionArg) {
        final Transaction transaction;
        if(null==transactionArg){
            transaction = getCurrentTransaction();
        }else {
            transaction = transactionArg;
        }
        transaction.changeStatus(TransactionStatus.CANCEL);
        this.updateTransaction(transaction);
        rollbackTransaction(transaction);
    }

    /**
     * 提交事务
     * @param transaction
     */
    private void commitTransaction(Transaction transaction) {
        try {
            transaction.commit();
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
        if(currentThreadLocal.get()!=null){
            return currentThreadLocal.get().peek();
        }
       return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = currentThreadLocal.get();
        return transactions != null && !transactions.isEmpty();
    }

    /**
     * 注册事务
     * @param transaction
     */
    private void registerTransaction(Transaction transaction) {
        if (currentThreadLocal.get() == null) {
            currentThreadLocal.set(new LinkedList<Transaction>());
        }
        currentThreadLocal.get().push(transaction);
    }

    public void registerTransactionTrace(Transaction transaction) {
        registerTransaction(transaction);
    }

    public void cleanAfterCompletion(Transaction transaction) {
        //clean(transaction);

    }

    /**
     * 清除缓存
     * @param transaction
     */
    public void cleanAfterConfirm(Transaction transaction) {
        this.putToConfirmTransactionCache(transaction);
        transactionRepository.delete(transaction);
        switch (TransactionType.valueOf(transaction.getTransactionType().value())){
            case ROOT:
                try {
                    CoordinatorWatcher.getInstance().deleteDataNodeForConfirm(transaction);
                } catch (Exception e) {
                }
                break;
            default:
                break;
        }
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
                currentThreadLocal.get().pop();
                if (currentThreadLocal.get().size() == 0) {
                    currentThreadLocal.remove();
                }
            } else {
                throw new TccSystemErrorException("本地线程事务缓存错误！");
            }
        }
    }

    /**
     * 删除缓存
     * @param transaction
     */
    public void cleanAfterCancel(Transaction transaction) {
        this.putToCancelTransactionCache(transaction);
        transactionRepository.delete(transaction);
        switch (TransactionType.valueOf(transaction.getTransactionType().value())){
            case ROOT:
                try {
                    CoordinatorWatcher.getInstance().deleteDataNodeForCancel(transaction);
                } catch (Exception e) {
                }
                break;
            default:
                break;
        }
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

    /**
     * 保存提交方法
     * @param transaction
     * @throws Exception
     */
    public void createConfirmOrder(Transaction transaction) throws Exception{
        transaction.changeStatus(TransactionStatus.CONFIRM);
        transactionRepository.createConfirmOrder(transaction);
    }

    /**
     * 向协调者发送提交指令
     * @param transaction
     * @throws Exception
     */
    public void sendConfirmOrder(Transaction transaction) throws Exception{
        transaction.changeStatus(TransactionStatus.CONFIRM);
        CoordinatorWatcher.getInstance().modify(transaction);
    }

    /**
     * 发送确认到事务组成员
     * @param transaction
     * @throws Exception
     */
    public void sendConfirmOrderToMember(Transaction transaction) throws Exception{
        //如果主调用方已经被其它线程执行取消事务，则不做任何操作
        if(peekCancelTransactionCache(transaction)!=null){
            return;
        }
        //如果给子系统、用户手动取消，则向协调者发出取消指令，否则执行Confirm
        if(!transactionRepository.isCanceling(transaction)){
            //创建confirm 事务组
            createGroupMemberForConfirm(transaction);
            //发送confirm 命令到各个事务组成员
            sendConfirmOrder(transaction);
        }else {
            log.error("TCC:事务已经给其它子系统或者用户取消");
            transaction.changeStatus(TransactionStatus.CANCEL);
            //事务组已经由其它方式创建，则只需通知事务组成员
            createAndSendCancelOrderToMember(transaction);
        }
    }

    public void updateTransaction(Transaction transaction){
        transactionRepository.update(transaction);
    }

    /**
     * 创建取消日志
     * @param transaction
     * @throws Exception
     */
    public void createCancelOrderToMember(Transaction transaction) throws Exception{
        transactionRepository.createRollbackOrder(transaction);
    }

    /**
     * 事务组成员执行取消命令
     * @param transaction
     * @throws Exception
     */
    public void sendCancelOrderToMember(Transaction transaction) throws Exception{
        transaction.changeStatus(TransactionStatus.CANCEL);
        CoordinatorWatcher.getInstance().modify(transaction);
    }

    /**
     * 发送并执行取消命令
     * @param transaction
     * @throws Exception
     */
    public void createAndSendCancelOrderToMember(Transaction transaction) throws Exception{
        //createCancelOrderToMember(transaction);
        sendCancelOrderToMember(transaction);
    }


}

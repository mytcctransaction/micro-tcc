package org.micro.tcc.tc.component;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.TransactionMember;
import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.common.exception.CancelException;
import org.micro.tcc.common.exception.ConfirmException;
import org.micro.tcc.common.exception.NoExistedTransactionException;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.tc.repository.RedisSpringTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;


@Slf4j
public class TransactionManager {

    static final Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    private TransactionRepository transactionRepository;

    private Cache<String, Transaction> TRANSACTION_CACHE = CacheBuilder.newBuilder().maximumSize(5000).expireAfterAccess(30L, TimeUnit.MINUTES).build();

    private static final ThreadLocal<Deque<Transaction>> CURRENT = new ThreadLocal<Deque<Transaction>>();

    private ExecutorService executorService;

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

        if (executorService == null) {

            synchronized (TransactionManager.class) {

                if (executorService == null) {
                    executorService = Executors.newCachedThreadPool();
                }
            }
        }
        this.executorService=executorService;

    }
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
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

    public Transaction propagationExistStart(TccTransactionContext transactionContext) throws NoExistedTransactionException {
        Transaction transaction = transactionRepository.findByGroupId(transactionContext.getXid().getGlobalTccTransactionId());
        if (transaction != null) {
            transaction.changeStatus(TransactionStatus.valueOf(transactionContext.getStatus()));
            registerTransaction(transaction);
            return transaction;
        } else {
            log.error("******************transaction is null**** at propagationExistStart*************");
            throw new NoExistedTransactionException("group id 不存在！");
        }
    }
    public boolean isExitGlobalTransaction(String groupId){
       Transaction transaction= peekCache(groupId);
       if(null!=transaction)return true;
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

    public void rollbackForClient() throws Exception {
       Transaction transaction =getCurrentTransaction();
        transaction.changeStatus(TransactionStatus.CANCEL);
        CoordinatorWatcher.modify(transaction);
    }

    public void commit(Transaction transactionArg,boolean asyncCommit) {
        final Transaction transaction;
        if(null==transactionArg){
            transaction = getCurrentTransaction();
        }else {
            transaction = transactionArg;
        }
        transaction.changeStatus(TransactionStatus.CONFIRM);
        transactionRepository.update(transaction);
        if (asyncCommit) {
            try {
                Long statTime = System.currentTimeMillis();
                executorService.submit(new Runnable() {
                    @Override
                    public void run() {
                        commitTransaction(transaction);
                    }
                });
                logger.debug("async submit cost time:" + (System.currentTimeMillis() - statTime));
            } catch (Throwable commitException) {
                logger.warn("TCC transaction async submit confirm failed, recovery job will try to confirm later.", commitException);
                throw new ConfirmException(commitException);
            }
        } else {
            commitTransaction(transaction);
        }
    }




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
                logger.warn("transaction async rollback failed, recovery job will try to rollback later.", rollbackException);
                throw new CancelException(rollbackException);
            }
        } else {

            rollbackTransaction(transaction);
        }
    }


    private void commitTransaction(Transaction transaction) {
        try {
            transaction.commit();
            transactionRepository.delete(transaction);
        } catch (Throwable commitException) {
            logger.warn(" transaction confirm failed, recovery job will try to confirm later.", commitException);
            throw new ConfirmException(commitException);
        }
    }

    private void rollbackTransaction(Transaction transaction) {
        try {
            transaction.rollback();
            transactionRepository.delete(transaction);
        } catch (Throwable rollbackException) {
            logger.warn(" transaction rollback failed, recovery job will try to rollback later.", rollbackException);
            //throw new CancelException(rollbackException);
        }
    }

    public Transaction getCurrentTransaction() {
        if(CURRENT.get()!=null)
            return CURRENT.get().peek();
       return null;
    }

    public boolean isTransactionActive() {
        Deque<Transaction> transactions = CURRENT.get();
        return transactions != null && !transactions.isEmpty();
    }


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
    public void cleanAfterCancel(Transaction transaction) {
        clean(transaction);
        delCache(transaction.getTransactionXid().getGlobalTccTransactionId());

    }

    public void enlistParticipant(TransactionMember participant) {
        Transaction transaction = this.getCurrentTransaction();
        transaction.enlistParticipant(participant);
        transactionRepository.update(transaction);
    }
}

//package org.micro.tcc.tc.recover;
//
//import com.alibaba.fastjson.JSON;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang.exception.ExceptionUtils;
//
//import org.micro.tcc.common.constant.TransactionStatus;
//import org.micro.tcc.common.constant.TransactionType;
//import org.micro.tcc.common.core.Transaction;
//import org.micro.tcc.common.core.TransactionRepository;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
//import java.util.Calendar;
//import java.util.Date;
//import java.util.List;
//
//@Slf4j
//public class TransactionRecovery {
//
//
//
//    public void beginRecover() {
//
//        List<Transaction> transactions = loadExceptionTransactions();
//
//        recoverExceptionTransactions(transactions);
//    }
//
//    private List<Transaction> loadExceptionTransactions() {
//
//
//        long currentTimeInMillis = Calendar.getInstance().getTimeInMillis();
//
//        TransactionRepository transactionRepository = transactionConfigurator.getTransactionRepository();
//        RecoverConfig recoverConfig = transactionConfigurator.getRecoverConfig();
//
//        return transactionRepository.findAll(new Date(currentTimeInMillis - recoverConfig.getRecoverDuration() * 1000));
//    }
//
//    private void recoverExceptionTransactions(List<Transaction> transactions) {
//
//
//        for (Transaction transaction : transactions) {
//
//            if (transaction.getRetriedCount() > transactionConfigurator.getRecoverConfig().getMaxRetryCount()) {
//
//                logger.error(String.format("recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getTransactionXid(), transaction.getStatus().value(), transaction.getRetriedCount(), JSON.toJSONString(transaction)));
//                continue;
//            }
//
//            if (transaction.getTransactionType().equals(TransactionType.BRANCH)
//                    && (transaction.getCreateTime().getTime() +
//                    transactionConfigurator.getRecoverConfig().getMaxRetryCount() *
//                            transactionConfigurator.getRecoverConfig().getRecoverDuration() * 1000
//                    > System.currentTimeMillis())) {
//                continue;
//            }
//
//            try {
//                transaction.addRetriedCount();
//
//                if (transaction.getStatus().equals(TransactionStatus.CONFIRM)) {
//
//                    transaction.changeStatus(TransactionStatus.CONFIRM);
//                    transactionConfigurator.getTransactionRepository().update(transaction);
//                    transaction.commit();
//                    transactionConfigurator.getTransactionRepository().delete(transaction);
//
//                } else if (transaction.getStatus().equals(TransactionStatus.CANCEL)
//                        || transaction.getTransactionType().equals(TransactionType.ROOT)) {
//
//                    transaction.changeStatus(TransactionStatus.CANCEL);
//                    transactionConfigurator.getTransactionRepository().update(transaction);
//                    transaction.rollback();
//                    transactionConfigurator.getTransactionRepository().delete(transaction);
//                }
//
//            } catch (Throwable throwable) {
//
//                if (throwable instanceof OptimisticLockException
//                        || ExceptionUtils.getRootCause(throwable) instanceof OptimisticLockException) {
//                    logger.warn(String.format("optimisticLockException happened while recover. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getTransactionXid(), transaction.getStatus().value(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
//                } else {
//                    logger.error(String.format("recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getTransactionXid(), transaction.getStatus().value(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);
//                }
//            }
//        }
//    }
//
//
//}

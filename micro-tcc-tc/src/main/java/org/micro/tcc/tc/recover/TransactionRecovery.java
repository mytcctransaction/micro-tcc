package org.micro.tcc.tc.recover;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.constant.TransactionType;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.core.TransactionRepository;
import org.micro.tcc.tc.repository.RedisSpringTransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Calendar;
import java.util.Date;
import java.util.List;

@Slf4j
public class TransactionRecovery {


    public void beginRecover() {
        log.debug("TCC:准备进行事务恢复");
        List<Transaction> transactions = loadExceptionTransactions();
        if(null!=transactions) recoverExceptionTransactions(transactions);
        log.debug("TCC:事务恢复成功结束");
    }

    private List<Transaction> loadExceptionTransactions() {
        long currentTimeInMillis = Calendar.getInstance().getTimeInMillis();

        TransactionRepository transactionRepository = RedisSpringTransactionRepository.getInstance();

        return transactionRepository.findAll(new Date(currentTimeInMillis));

    }

    private void recoverExceptionTransactions(List<Transaction> transactions) {

        TransactionRepository transactionRepository = RedisSpringTransactionRepository.getInstance();
        for (Transaction transaction : transactions) {

            if (transaction.getRetriedCount() > TransactionRecoverConfig.getInstance().getMaxRetryCount()) {

                log.error(String.format("TCC:recover failed with max retry count,will not try again. txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getTransactionXid(), transaction.getStatus().value(), transaction.getRetriedCount(), JSON.toJSONString(transaction)));
                continue;
            }
            if (TransactionRecoverConfig.getInstance().getMaxRetryCount()>5 && (transaction.getCreateTime().getTime() +
                    TransactionRecoverConfig.getInstance().getMaxRetryCount() *
                            TransactionRecoverConfig.getInstance().getRecoverDuration() > System.currentTimeMillis())){
                continue;
            }

            try {
                transaction.addRetriedCount();
                if (transaction.getStatus().equals(TransactionStatus.CONFIRM)) {
                    //transaction.changeStatus(TransactionStatus.CONFIRM);
                    transactionRepository.update(transaction);
                    transaction.commit();
                    transactionRepository.delete(transaction);

                } else if (transaction.getStatus().equals(TransactionStatus.CANCEL)) {
                    //transaction.changeStatus(TransactionStatus.CANCEL);
                    transactionRepository.update(transaction);
                    transaction.rollback();
                    transactionRepository.delete(transaction);
                }

            } catch (Throwable throwable) {
                log.error(String.format("TCC:recover failed, txid:%s, status:%s,retried count:%d,transaction content:%s", transaction.getTransactionXid(), transaction.getStatus().value(), transaction.getRetriedCount(), JSON.toJSONString(transaction)), throwable);

            }
        }
    }


}

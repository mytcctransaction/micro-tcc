package org.micro.tcc.tc.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;

import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;

import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.tc.component.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.micro.tcc.tc.component.CoordinatorWatcher;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
@Component
public class TccTransactionInterceptor {

    static final Logger logger = LoggerFactory.getLogger(TccTransactionInterceptor.class.getSimpleName());


    private TransactionManager transactionManager=TransactionManager.getInstance();


    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public Object interceptTransactionMethod(ProceedingJoinPoint pjp) throws Throwable {

        TccMethodContext tccMethodContext = new TccMethodContext(pjp);
        //获取feign传输过来的gid
        Transaction transaction= TransactionManager.getInstance().getCurrentTransaction();
        boolean isTransactionActive=false;
        if(null!=transaction){
            isTransactionActive=true;
        }
        switch (tccMethodContext.getMethodRole(isTransactionActive)) {
            case ROOT:
                return processRoot(tccMethodContext);
            case PROVIDER:
                TccTransactionContext transactionContext=new TccTransactionContext(transaction.getTransactionXid(),transaction.getStatus().value());
                tccMethodContext.transactionContext=transactionContext;

                return processProvider(tccMethodContext);
            default:
                return pjp.proceed();
        }
    }


    private Object processRoot(TccMethodContext tccMethodContext) throws Throwable {
        Object returnValue = null;
        Transaction transaction = null;
        boolean asyncConfirm = tccMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = tccMethodContext.getAnnotation().asyncCancel();
        long a=System.currentTimeMillis();
        try {
            //transaction = transactionManager.begin(tccMethodContext.getUniqueIdentity());
            transaction = transactionManager.begin();
             long b=System.currentTimeMillis();
            logger.error("processRoot-time0:{}",b-a);
            a=System.currentTimeMillis();
            CoordinatorWatcher.getInstance().add(transaction);
             b=System.currentTimeMillis();
            logger.error("processRoot-time1:{}",b-a);
            try {
                a=System.currentTimeMillis();
                returnValue = tccMethodContext.proceed();
                 b=System.currentTimeMillis();
                logger.error("processRoot-time2:{}",b-a);
            } catch (Throwable tryingException) {
                transaction.changeStatus(TransactionStatus.CANCEL);
                //主调用方失败，修改状态为cancel，次调用方需要全部回滚
                CoordinatorWatcher.getInstance().modify(transaction);
                throw tryingException;
            }
            try{
                a=System.currentTimeMillis();
                //发出提交指令，所有子系统执行提交
                transaction.changeStatus(TransactionStatus.CONFIRM);
                CoordinatorWatcher.getInstance().modify(transaction);
                 b=System.currentTimeMillis();
                logger.error("processRoot-time3:{}",b-a);
            }catch (Throwable t){
                //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                transaction.changeStatus(TransactionStatus.CANCEL);
                CoordinatorWatcher.getInstance().modify(transaction);
                throw t;
            }
        } finally {
            transactionManager.cleanAfterCompletion(transaction);
        }
        return returnValue;
    }

    private Object processProvider(TccMethodContext tccMethodContext) throws Throwable {
        Transaction transaction = null;
        boolean asyncConfirm = tccMethodContext.getAnnotation().asyncConfirm();
        boolean asyncCancel = tccMethodContext.getAnnotation().asyncCancel();
        long a=System.currentTimeMillis();
        Object returnObj=null;
        try {
            switch (TransactionStatus.valueOf(tccMethodContext.getTransactionContext().getStatus())) {
                case TRY:
                    transaction = transactionManager.propagationSupportsStart(tccMethodContext.getTransactionContext());
                    CoordinatorWatcher.getInstance().add(transaction);
                    try {
                        returnObj= tccMethodContext.proceed();
                    }catch (Throwable t){
                        //TODO
                        //调用方commit失败，修改状态为cancel，所有调用方需要全部回滚
                        transaction.changeStatus(TransactionStatus.CANCEL);
                        CoordinatorWatcher.getInstance().modify(transaction);
                        throw t;
                    }

                case CONFIRM:

                    break;
                case CANCEL:

                    break;
                default:
                    break;
            }

        } finally {
            long b=System.currentTimeMillis();
            logger.error("processProvider-time:{}",b-a);
            transactionManager.cleanAfterCompletion(transaction);
        }
        return returnObj;
    }



}

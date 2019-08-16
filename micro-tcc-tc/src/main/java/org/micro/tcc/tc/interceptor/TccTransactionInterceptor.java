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

        try {
            //transaction = transactionManager.begin(tccMethodContext.getUniqueIdentity());
            transaction = transactionManager.begin();
            CoordinatorWatcher.add(transaction);
            try {
                returnValue = tccMethodContext.proceed();
            } catch (Throwable tryingException) {

                transaction.changeStatus(TransactionStatus.CANCEL);
                //主调用方失败，修改状态为cancel，次调用方需要全部回滚
                CoordinatorWatcher.modify(transaction);
                throw tryingException;
            }
            try{
                transaction.changeStatus(TransactionStatus.CONFIRM);
                CoordinatorWatcher.modify(transaction);
            }catch (Throwable t){
                //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                transaction.changeStatus(TransactionStatus.CANCEL);
                CoordinatorWatcher.modify(transaction);
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
        Object returnObj=null;
        try {
            switch (TransactionStatus.valueOf(tccMethodContext.getTransactionContext().getStatus())) {
                case TRY:
                    transaction = transactionManager.propagationSupportsStart(tccMethodContext.getTransactionContext());
                    CoordinatorWatcher.add(transaction);
                    try {
                        returnObj= tccMethodContext.proceed();
                    }catch (Throwable t){
                        //TODO
                        //调用方commit失败，修改状态为cancel，所有调用方需要全部回滚
                        transaction.changeStatus(TransactionStatus.CANCEL);
                        CoordinatorWatcher.modify(transaction);
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
            transactionManager.cleanAfterCompletion(transaction);
        }
        return returnObj;
    }



}

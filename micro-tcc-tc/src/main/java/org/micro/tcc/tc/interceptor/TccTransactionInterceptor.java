package org.micro.tcc.tc.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;

import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;

import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.exception.CancelException;
import org.micro.tcc.tc.component.TransactionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.micro.tcc.tc.component.CoordinatorWatcher;

/**
 *@author jeff.liu
 *  tcc 事务主要拦截器，主要拦截器
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
            //transactionManager.addGroupForMember(transaction);
            try {
                returnValue = tccMethodContext.proceed();
                transactionManager.saveConfirmOrder(transaction);
            } catch (Throwable tryingException) {
                if(tryingException instanceof CancelException){
                    throw tryingException;
                }
                //主调用方失败，修改状态为cancel，次调用方需要全部回滚
                transactionManager.sendCancelOrderToMember(transaction);
                throw new CancelException(tryingException);
            }
            try{
                //发出提交指令，所有子系统执行提交
                transactionManager.sendConfirmOrderToMember(transaction);

            }catch (Throwable t){
                if(t instanceof CancelException){
                    throw t;
                }
                //主调用方commit失败，修改状态为cancel，次调用方需要全部回滚
                transactionManager.sendCancelOrderToMember(transaction);
                throw new CancelException(t);
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
                    //transactionManager.addGroupForMember(transaction);
                    try {
                        returnObj= tccMethodContext.proceed();
                        transactionManager.saveConfirmOrder(transaction);
                    }catch (Throwable t){
                        if(t instanceof CancelException){
                            throw t;
                        }
                        //TODO
                        //调用方commit失败，修改状态为cancel，所有调用方需要全部回滚
                        transactionManager.sendCancelOrderToMember(transaction);
                        throw new CancelException(t);
                    }
                    break;
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

package org.micro.tcc.tc.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.micro.tcc.common.core.TccTransactionContext;
import org.micro.tcc.common.core.Transaction;
import org.micro.tcc.common.constant.TransactionStatus;
import org.micro.tcc.common.exception.CancelException;
import org.micro.tcc.tc.component.TransactionManager;
import org.springframework.stereotype.Component;

/**
 *@author jeff.liu
 *  tcc 事务拦截器，主要拦截器
 * date 2019/7/31
 */
@Component
@Slf4j
public class TccTransactionInterceptor {

    private TransactionManager transactionManager= TransactionManager.getInstance();


    public void setTransactionManager(TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    /**
     * 拦截业务方法
     * 判断是初始化事务还是加入事务
     * @param pjp
     * @return
     * @throws Throwable
     */
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
                return processPropagationRequired(tccMethodContext);
            case PROVIDER:
                TccTransactionContext transactionContext=new TccTransactionContext(transaction.getTransactionXid(),transaction.getStatus().value());
                tccMethodContext.transactionContext=transactionContext;
                return processPropagationSupports(tccMethodContext);
            default:
                return pjp.proceed();
        }
    }

    /**
     * 判断抛出异常，如果是自定义异常，则回滚
     * @param transaction
     * @param tryingException
     * @throws Throwable
     */
    private  void checkThrowsException(Transaction transaction,Throwable tryingException) throws Throwable{
        Class<? extends Throwable>[] rollbackFor=transaction.getRollbackFor();
        for(Class c:rollbackFor){
            //抛出的异常继承rollbackFor
            if(c.isAssignableFrom(tryingException.getClass())){
                //主调用方失败，修改状态为cancel，次调用方需要全部回滚
                //创建cancel事务组
                transactionManager.createGroupMemberForCancel(transaction);
                //通知事务组成员执行取消
                transactionManager.createAndSendCancelOrderToMember(transaction);
                throw new CancelException(tryingException);
            }
        }
    }

    /**
     * 处理初始化事务
     * @param tccMethodContext
     * @return
     * @throws Throwable
     */
    private Object processPropagationRequired(TccMethodContext tccMethodContext) throws Throwable {
        Object returnValue = null;
        Transaction transaction = null;
        try {
            transaction = transactionManager.propagationRequiredBegin(tccMethodContext);
            try {
                returnValue = tccMethodContext.proceed();
            } catch (Throwable tryingException) {
                if(tryingException instanceof CancelException){
                    throw tryingException;
                }
                checkThrowsException(transaction,tryingException);
            }
            try{
                //发出提交指令，所有子系统执行提交
                transactionManager.sendConfirmOrderToMember(transaction);
            }catch (Throwable t){
                checkThrowsException(transaction,t);
            }
        } finally {
            transactionManager.cleanAfterCompletion(transaction);
        }
        return returnValue;
    }

    /**
     * 处理成员事务，即是加入事务
     * @param tccMethodContext
     * @return
     * @throws Throwable
     */
    private Object processPropagationSupports(TccMethodContext tccMethodContext) throws Throwable {
        Transaction transaction = null;
        Object returnObj=null;
        try {
            switch (TransactionStatus.valueOf(tccMethodContext.getTransactionContext().getStatus())) {
                case TRY:
                    transaction = transactionManager.propagationSupportsBegin(tccMethodContext);
                    try {
                        returnObj= tccMethodContext.proceed();
                    }catch (Throwable t){
                        if(t instanceof CancelException){
                            throw t;
                        }
                        Class<? extends Throwable>[] rollbackFor=transaction.getRollbackFor();
                        for(Class c:rollbackFor){
                            //抛出的异常继承rollbackFor
                            if(c.isAssignableFrom(t.getClass())){
                                //创建cancel事务组
                                transactionManager.createGroupMemberForCancel(transaction);
                                //次调用方失败，修改日志状态为cancel
                                //transactionManager.createCancelOrderToMember(transaction);
                                throw new CancelException(t);
                            }
                        }
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

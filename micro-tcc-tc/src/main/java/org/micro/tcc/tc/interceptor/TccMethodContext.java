package org.micro.tcc.tc.interceptor;

import org.aspectj.lang.ProceedingJoinPoint;

import org.micro.tcc.common.constant.Propagation;

import org.micro.tcc.common.core.TccTransactionContext;

import org.micro.tcc.common.constant.MethodType;
import org.micro.tcc.tc.util.ReflectionUtils;
import org.micro.tcc.tc.annotation.TccIdentity;
import org.micro.tcc.tc.annotation.TccTransaction;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

/**
 *@author jeff.liu
 *   tcc 方法内容
 * date 2019/7/31
 */
public class TccMethodContext {

    ProceedingJoinPoint pjp = null;

    Method method = null;

    TccTransaction tccTransaction = null;

    Propagation propagation = null;

    TccTransactionContext transactionContext = null;

    public TccMethodContext(ProceedingJoinPoint pjp) {
        this.pjp = pjp;
        this.method = ReflectionUtils.getTccTransactionMethod(pjp);
        this.tccTransaction = method.getAnnotation(TccTransaction.class);
        this.propagation = tccTransaction.propagation();

    }

    public TccTransaction getAnnotation() {
        return tccTransaction;
    }

    public Propagation getPropagation() {
        return propagation;
    }

    public TccTransactionContext getTransactionContext() {
        return transactionContext;
    }

    public Method getMethod() {
        return method;
    }

    public Object getUniqueIdentity() {
        Annotation[][] annotations = this.getMethod().getParameterAnnotations();

        for (int i = 0; i < annotations.length; i++) {
            for (Annotation annotation : annotations[i]) {
                if (annotation.annotationType().equals(TccIdentity.class)) {

                    Object[] params = pjp.getArgs();
                    Object unqiueIdentity = params[i];

                    return unqiueIdentity;
                }
            }
        }

        return null;
    }




    public MethodType getMethodRole(boolean isTransactionActive) {
        if ((propagation.equals(Propagation.REQUIRED) ) ||
                propagation.equals(Propagation.REQUIRES_NEW)) {
            return MethodType.ROOT;
        } else if ((propagation.equals(Propagation.SUPPORTS) || propagation.equals(Propagation.MANDATORY))&& isTransactionActive ) {
            return MethodType.PROVIDER;
        } else {
            return MethodType.NORMAL;
        }
    }

    public Object proceed() throws Throwable {
        return this.pjp.proceed();
    }
}
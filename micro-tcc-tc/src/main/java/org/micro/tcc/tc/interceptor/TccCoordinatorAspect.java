package org.micro.tcc.tc.interceptor;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 *@author jeff.liu
 *  参与者拦截器
 * date 2019/7/31
 */
@Component
@Aspect
@Order(2)
@Slf4j
public  class TccCoordinatorAspect {

    private TccCoordinatorInterceptor tccCoordinatorInterceptor;

    @Pointcut("@annotation(org.micro.tcc.tc.annotation.TccTransaction)")
    public void transactionContextCall() {

    }

    @Around("transactionContextCall()")
    public Object interceptTransactionContextMethod(ProceedingJoinPoint pjp) throws Throwable {
        log.debug("TCC:CoordinatorAspect ");
        if(null== tccCoordinatorInterceptor) tccCoordinatorInterceptor =new TccCoordinatorInterceptor();
        return tccCoordinatorInterceptor.interceptTransactionContextMethod(pjp);
    }

    public void setTccCoordinatorInterceptor(TccCoordinatorInterceptor tccCoordinatorInterceptor) {
        this.tccCoordinatorInterceptor = tccCoordinatorInterceptor;
    }


}

package org.micro.tcc.tc.component;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.reflect.MethodSignature;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Date;
import java.util.HashMap;

/**
*@author jeff.liu
*@desc   记录日志
*@date 2019/8/14
*/
@Slf4j
public class LogAspect {



	/*@Before("execution(* *..car.service..*.*(..))")
	public void doBeforeInServiceLayer(JoinPoint joinPoint) {
		log.debug("doBeforeInServiceLayer");
	}

	@After("execution(* *..car.service..*.*(..))")
	public void doAfterInServiceLayer(JoinPoint joinPoint) {
		log.debug("doAfterInServiceLayer");
	}*/

	@Around("execution(* *..tcc.tc.interceptor..*.*(..))")
	public Object doAround(ProceedingJoinPoint pjp) throws Throwable {
		// 开始时间
		long startTime = 0L;
		// 结束时间
		long endTime = 0L;
		startTime = System.currentTimeMillis();
		Signature signature = pjp.getSignature();
		MethodSignature methodSignature = (MethodSignature) signature;
		Method method = methodSignature.getMethod();
		Object result = pjp.proceed();
		endTime = System.currentTimeMillis();
		log.error("doAround>>>method={},result={},耗时2：{}", method.getName(),result, endTime - startTime);
		return result;
	}

}

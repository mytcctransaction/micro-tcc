package org.micro.tcc.common.core;


import org.apache.commons.lang.StringUtils;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.common.support.BeanFactoryBuilder;

import java.io.Serializable;
import java.lang.reflect.Method;


public class MethodReflect implements Serializable {

    private static final long serialVersionUID = -164958655471605778L;


    public MethodReflect() {

    }

    public Object invoke(TccTransactionContext transactionContext, Invocation invocation) {


        if (StringUtils.isNotEmpty(invocation.getMethodName())) {

            try {

                Object target = BeanFactoryBuilder.factoryBean(invocation.getTargetClass()).getInstance();

                Method method = null;

                method = target.getClass().getMethod(invocation.getMethodName(), invocation.getParameterTypes());

                return method.invoke(target, invocation.getArgs());

            } catch (Exception e) {
                throw new TccSystemErrorException(e);
            }
        }
        return null;
    }
}

package org.micro.tcc.common.core;


import com.esotericsoftware.reflectasm.MethodAccess;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.common.exception.TccSystemErrorException;
import org.micro.tcc.common.support.BeanFactoryBuilder;

import java.io.Serializable;
import java.lang.reflect.Method;

/**
*@author jeff.liu
*@desc   反射类
*@date 2019/8/20
*/
@Slf4j
public class MethodReflect implements Serializable {

    private static final long serialVersionUID = -164958655471605778L;


    public MethodReflect() {

    }

    public Object invoke(TccTransactionContext transactionContext, Invocation invocation) {


        if (StringUtils.isNotEmpty(invocation.getMethodName())) {

            try {
                long a=System.currentTimeMillis();

                Object target = BeanFactoryBuilder.factoryBean(invocation.getTargetClass()).getInstance();
                long b=System.currentTimeMillis();
                log.error("MethodReflect-time1:{}",b-a);
                Method method = null;
                a=System.currentTimeMillis();
                //生成字节码的方式创建MethodAccess
                MethodAccess access = MethodAccess.get(target.getClass());
                Object oo=access.invoke(target,invocation.getMethodName(), invocation.getParameterTypes(), invocation.getArgs());
                b=System.currentTimeMillis();
                log.error("MethodReflect-time2:{},method:{}",b-a,invocation.getMethodName());
                // method = target.getClass().getMethod(invocation.getMethodName(), invocation.getParameterTypes());
                // Object o= method.invoke(target, invocation.getArgs());
                return oo;

            } catch (Exception e) {
                throw new TccSystemErrorException(e);
            }
        }
        return null;
    }
}

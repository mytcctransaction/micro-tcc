package org.micro.tcc.common.util;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.micro.tcc.common.annotation.TccTransaction;

import java.lang.reflect.Method;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
public class ReflectionUtils {

    public static Method getTccTransactionMethod(ProceedingJoinPoint pjp) {
        Method method = ((MethodSignature) (pjp.getSignature())).getMethod();

        if (method.getAnnotation(TccTransaction.class) == null) {
            try {
                method = pjp.getTarget().getClass().getMethod(method.getName(), method.getParameterTypes());
            } catch (NoSuchMethodException e) {
                return null;
            }
        }
        return method;
    }


    public static Class getDeclaringType(Class aClass, String methodName, Class<?>[] parameterTypes) {

        Method method = null;

        Class findClass = aClass;

        do {
            Class[] clazzes = findClass.getInterfaces();

            for (Class clazz : clazzes) {

                try {
                    method = clazz.getDeclaredMethod(methodName, parameterTypes);
                } catch (NoSuchMethodException e) {
                    method = null;
                }

                if (method != null) {
                    return clazz;
                }
            }

            findClass = findClass.getSuperclass();

        } while (!findClass.equals(Object.class));

        return aClass;
    }


}

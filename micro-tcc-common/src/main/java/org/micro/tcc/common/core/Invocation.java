package org.micro.tcc.common.core;

import java.io.Serializable;


/**
*@author jeff.liu
*@desc   调用管理者
*@date 2019/8/27
*/
public class Invocation implements Serializable {


    private static final long serialVersionUID = -1921572071621055035L;
    /**
     * 目标类
     */
    private Class targetClass;

    /**
     * 方法
     */
    private String methodName;

    /**
     * 参数
     */
    private Class[] parameterTypes;

    private Object[] args;

    public Invocation() {

    }

    public Invocation(Class targetClass, String methodName, Class[] parameterTypes, Object... args) {
        this.methodName = methodName;
        this.parameterTypes = parameterTypes;
        this.targetClass = targetClass;
        this.args = args;
    }

    public Object[] getArgs() {
        return args;
    }

    public Class getTargetClass() {
        return targetClass;
    }

    public String getMethodName() {
        return methodName;
    }

    public Class[] getParameterTypes() {
        return parameterTypes;
    }
}

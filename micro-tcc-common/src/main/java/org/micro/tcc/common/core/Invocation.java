package org.micro.tcc.common.core;

import java.io.Serializable;


public class Invocation implements Serializable {


    private static final long serialVersionUID = -1921572071621055035L;
    private Class targetClass;

    private String methodName;

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

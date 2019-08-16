package org.micro.tcc.common.core;


import org.micro.tcc.common.constant.TransactionStatus;

import java.io.Serializable;


public class TransactionMember implements Serializable {


    private static final long serialVersionUID = 3284377143420086541L;
    private TransactionXid xid;

    private Invocation confirmInvocation;

    private Invocation cancelInvocation;

    private MethodReflect methodReflect = new MethodReflect();


    public TransactionMember() {

    }

    public TransactionMember(TransactionXid xid, Invocation confirmInvocation, Invocation cancelInvocation) {
        this.xid = xid;
        this.confirmInvocation = confirmInvocation;
        this.cancelInvocation = cancelInvocation;

    }

    public TransactionMember(Invocation confirmInvocation, Invocation cancelInvocation) {
        this.confirmInvocation = confirmInvocation;
        this.cancelInvocation = cancelInvocation;

    }

    public void setXid(TransactionXid xid) {
        this.xid = xid;
    }

    public void rollback() {
        methodReflect.invoke(new TccTransactionContext(xid, TransactionStatus.CANCEL.value()), cancelInvocation);
    }

    public void commit() {
        methodReflect.invoke(new TccTransactionContext(xid, TransactionStatus.CONFIRM.value()), confirmInvocation);
    }

    public MethodReflect getMethodReflect() {
        return methodReflect;
    }

    public TransactionXid getXid() {
        return xid;
    }

    public Invocation getConfirmInvocation() {
        return confirmInvocation;
    }

    public Invocation getCancelInvocation() {
        return cancelInvocation;
    }

}

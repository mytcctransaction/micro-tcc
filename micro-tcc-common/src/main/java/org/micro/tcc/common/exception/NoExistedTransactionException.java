package org.micro.tcc.common.exception;


public class NoExistedTransactionException extends Exception {
    private static final long serialVersionUID = 1031919168789207713L;

    public NoExistedTransactionException(String msg){
        super(msg);
    }
}

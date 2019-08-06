package org.micro.tcc.common.exception;


public class TccSystemErrorException extends RuntimeException {

    public TccSystemErrorException(String message) {
        super(message);
    }

    public TccSystemErrorException(Throwable e) {
        super(e);
    }

    public TccSystemErrorException(String message, Throwable e) {
        super(message, e);
    }
}

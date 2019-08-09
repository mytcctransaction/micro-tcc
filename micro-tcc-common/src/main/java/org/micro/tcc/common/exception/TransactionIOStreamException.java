package org.micro.tcc.common.exception;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
public class TransactionIOStreamException extends RuntimeException {

    private static final long serialVersionUID = 6508064607297986329L;

    public TransactionIOStreamException(String message) {
        super(message);
    }

    public TransactionIOStreamException(Throwable e) {
        super(e);
    }
}

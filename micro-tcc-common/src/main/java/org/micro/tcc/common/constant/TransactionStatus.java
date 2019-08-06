package org.micro.tcc.common.constant;


public enum TransactionStatus {

    TRY(1), CONFIRM(2), CANCEL(3);

    private int id;

     TransactionStatus(int id) {
        this.id = id;
    }

    public int value() {
        return id;
    }
    public void setId(int id) {
        this.id= id;
    }
    public static TransactionStatus valueOf(int id) {

        switch (id) {
            case 1:
                return TRY;
            case 2:
                return CONFIRM;
            default:
                return CANCEL;
        }
    }

}

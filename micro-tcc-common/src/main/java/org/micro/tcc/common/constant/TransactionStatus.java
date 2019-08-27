package org.micro.tcc.common.constant;

/**
*@author jeff.liu
*@desc   事务状态
*@date 2019/8/27
*/
public enum TransactionStatus {

    /**
     * 尝试发起
     */
    TRY(1),
    /**
     * 事务确认
     */
    CONFIRM(2),
    /**
     * 事务取消
     */
    CANCEL(3);

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

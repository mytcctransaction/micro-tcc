package org.micro.tcc.common.constant;


/**
*@author jeff.liu
*@desc   事务模式
*@date 2019/8/27
*/
public enum Model {

    /**
     * confirm发生异常自动rollback
     */
    AT(0),

    /**
     *  confirm发生异常不会自动rollback
     */
    MT(1);

    private final int value;

    private Model(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }
}
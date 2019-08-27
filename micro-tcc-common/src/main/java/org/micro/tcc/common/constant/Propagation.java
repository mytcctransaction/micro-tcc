package org.micro.tcc.common.constant;


/**
*@author jeff.liu
*@desc   事务类型
*@date 2019/8/27
*/
public enum Propagation {
    /**
     * 新建事务
     */
    REQUIRED(0),
    /**
     * 加入事务
     */
    SUPPORTS(1),
    MANDATORY(2),
    REQUIRES_NEW(3);

    private final int value;

    private Propagation(int value) {
        this.value = value;
    }

    public int value() {
        return this.value;
    }
}
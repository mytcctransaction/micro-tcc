

package org.micro.tcc.common.constant;

/**
*@author jeff.liu
*@desc   事务类型
*@date 2019/8/27
*/
public enum TransactionType {

    /**
     * 发起者
     */
    ROOT(1),
    /**
     * 参与者
     */
    BRANCH(2);
    int id;


    public int value(){
        return id;
    }
    TransactionType(int id) {
        this.id = id;
    }


    public static TransactionType valueOf(int id) {
        switch (id) {
            case 1:
                return ROOT;
            case 2:
                return BRANCH;
            default:
                return null;
        }
    }

}

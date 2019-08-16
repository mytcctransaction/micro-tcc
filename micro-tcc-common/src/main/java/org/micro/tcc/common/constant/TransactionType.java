

package org.micro.tcc.common.constant;


public enum TransactionType {

    ROOT(1),
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

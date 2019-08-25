package org.micro.tcc.common.constant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
*@author jeff.liu
*@desc  静态变量
*@date 2019/8/15
*/
@Component
public  class  Constant {


    public  static String transactionMapKey;
    public static String prefix="TCC:";
    public static String ROOT=":ROOT";
    public static String BRANCH=":BRANCH";
    public static String TRY="TRY";
    public static String CONFIRM="CONFIRM";
    public static String CANCEL="CANCEL";
    public final static String GLOBAL_TCCTRANSACTION_ID="globalTccTransactionId";
    public final static String TCCTRANSACTION_STATUS="tccTransactionStatus";

    public final static String TRANSACTION_GROUP="TRANSACTION_GROUP:";

    public static String getTransactionMapKey(){
        return prefix+transactionMapKey;
    }

    public final static String DELIMIT="#";

    public static String getAppName(){
        return transactionMapKey;
    }
    @Value("${spring.application.name}")
    public void setTransactionMapKey(String transactionMapKey) {
        Constant.transactionMapKey = transactionMapKey;
    }
}

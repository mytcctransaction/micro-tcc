package org.micro.tcc.common.constant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public  class  Constant {


    public  static String transactionMapKey;
    private static String prefix="TCC:";
    public static String ROOT=":ROOT";
    public static String BRANCH=":BRANCH";
    public final static String GLOBAL_TCCTRANSACTION_ID="globalTccTransactionId";
    public final static String TCCTRANSACTION_STATUS="tccTransactionStatus";
    public static String getTransactionMapKey(){
        return prefix+transactionMapKey;
    }

    public static String getAppName(){
        return transactionMapKey;
    }
    @Value("${spring.application.name}")
    public void setTransactionMapKey(String transactionMapKey) {
        this.transactionMapKey = transactionMapKey;
    }
}

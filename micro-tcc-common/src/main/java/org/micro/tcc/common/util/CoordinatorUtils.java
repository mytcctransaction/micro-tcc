package org.micro.tcc.common.util;

import org.micro.tcc.common.constant.Constant;

public class CoordinatorUtils {

    public static String getNodeGroupId(String transactionPath){
        String[] path=transactionPath.split("\\"+Constant.DELIMIT);
        if(path.length==2){
            return path[0];
        }
        return "";
    }

    public static String getAppName(String transactionPath){

        return getNodeGroupId(transactionPath);
    }
}

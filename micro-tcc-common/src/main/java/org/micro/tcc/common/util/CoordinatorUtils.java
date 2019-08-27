package org.micro.tcc.common.util;

import org.micro.tcc.common.constant.Constant;

/**
*@author jeff.liu
*@desc   协调工具
*@date 2019/8/27
*/
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

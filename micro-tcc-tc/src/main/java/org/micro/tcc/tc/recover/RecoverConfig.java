package org.micro.tcc.tc.recover;

import java.util.Set;

/**
 *@author jeff.liu
 *   事务恢复配置接口
 * date 2019/7/31
 */
public interface RecoverConfig {

    public int getMaxRetryCount();

    public int getRecoverDuration();

    public String getCronExpression();




}

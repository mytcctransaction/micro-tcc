package org.micro.tcc.tc.recover;



import java.net.SocketTimeoutException;
import java.util.HashSet;
import java.util.Set;


public class TransactionRecoverConfig implements RecoverConfig {

    public static final TransactionRecoverConfig INSTANCE = new TransactionRecoverConfig();

    private int maxRetryCount = 30;

    //160 秒
    private int recoverDuration = 160;

    private String cronExpression = "0 */1 * * * ?";

    public static TransactionRecoverConfig getInstance(){
        return INSTANCE;
    }



    public TransactionRecoverConfig() {

    }

    @Override
    public int getMaxRetryCount() {
        return maxRetryCount;
    }

    @Override
    public int getRecoverDuration() {
        return recoverDuration;
    }

    @Override
    public String getCronExpression() {
        return cronExpression;
    }


    public void setMaxRetryCount(int maxRetryCount) {
        this.maxRetryCount = maxRetryCount;
    }

    public void setRecoverDuration(int recoverDuration) {
        this.recoverDuration = recoverDuration;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }


}

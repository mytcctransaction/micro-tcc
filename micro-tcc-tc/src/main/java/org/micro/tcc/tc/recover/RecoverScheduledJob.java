package org.micro.tcc.tc.recover;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.tc.component.SpringContextAware;
import org.quartz.Scheduler;
import org.quartz.SchedulerFactory;
import org.quartz.impl.StdSchedulerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.scheduling.quartz.CronTriggerFactoryBean;
import org.springframework.scheduling.quartz.MethodInvokingJobDetailFactoryBean;


/**
*@author jeff.liu
*@desc   事务恢复定时任务
*@date 2019/8/27
*/
@Slf4j
public class RecoverScheduledJob implements ApplicationRunner {

    private TransactionRecovery transactionRecovery=new TransactionRecovery();


    private Scheduler scheduler;

    public void init() {

        try {


            TransactionRecoverConfig transactionConfigurator= SpringContextAware.getBean(TransactionRecoverConfig.class);
            if(StringUtils.isEmpty(transactionConfigurator.getCronExpression())){
                return;
            }
            MethodInvokingJobDetailFactoryBean jobDetail = new MethodInvokingJobDetailFactoryBean();
            jobDetail.setTargetObject(transactionRecovery);
            jobDetail.setTargetMethod("beginRecover");
            jobDetail.setName("transactionRecoveryJob");
            jobDetail.setConcurrent(false);
            jobDetail.afterPropertiesSet();

            CronTriggerFactoryBean cronTrigger = new CronTriggerFactoryBean();
            cronTrigger.setBeanName("transactionRecoveryCronTrigger");
            cronTrigger.setCronExpression(transactionConfigurator.getCronExpression());
            cronTrigger.setJobDetail(jobDetail.getObject());
            cronTrigger.afterPropertiesSet();
            SchedulerFactory schedulerfactory = new StdSchedulerFactory();
            scheduler=schedulerfactory.getScheduler();
            scheduler.scheduleJob(jobDetail.getObject(), cronTrigger.getObject());
            scheduler.start();
            log.info("TCC:开启分布式事务定时恢复任务");

        } catch (Exception e) {
            log.error("TCC:定时commit/cancel事务发生异常！",e);
        }
    }

    public void setTransactionRecovery(TransactionRecovery transactionRecovery) {
        this.transactionRecovery = transactionRecovery;
    }



    @Override
    public void run(ApplicationArguments args) throws Exception {
        init();
    }
}

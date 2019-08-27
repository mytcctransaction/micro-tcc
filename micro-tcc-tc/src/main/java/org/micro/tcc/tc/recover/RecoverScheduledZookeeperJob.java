package org.micro.tcc.tc.recover;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.micro.tcc.tc.component.CoordinatorWatcher;
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
*@desc   zk事件调用
*@date 2019/8/19
*/
@Slf4j
public class RecoverScheduledZookeeperJob implements ApplicationRunner {


    private Scheduler scheduler;

    public void init() {

        TransactionRecoverConfig transactionConfigurator= SpringContextAware.getBean(TransactionRecoverConfig.class);

        try {
            if(StringUtils.isEmpty(transactionConfigurator.getCronZkExpression())){
                return;
            }

            CoordinatorWatcher coordinatorWatcher= SpringContextAware.getBean(CoordinatorWatcher.class);
            MethodInvokingJobDetailFactoryBean jobDetail = new MethodInvokingJobDetailFactoryBean();
            jobDetail.setTargetObject(coordinatorWatcher);
            jobDetail.setTargetMethod("processTransactionStart");
            jobDetail.setName("transactionRecoveryZKJob");
            jobDetail.setConcurrent(false);
            jobDetail.afterPropertiesSet();

            CronTriggerFactoryBean cronTrigger = new CronTriggerFactoryBean();
            cronTrigger.setBeanName("transactionRecoveryZKCronTrigger");
            cronTrigger.setCronExpression(transactionConfigurator.getCronZkExpression());
            cronTrigger.setJobDetail(jobDetail.getObject());
            cronTrigger.afterPropertiesSet();
            SchedulerFactory schedulerfactory = new StdSchedulerFactory();
            scheduler=schedulerfactory.getScheduler();
            scheduler.scheduleJob(jobDetail.getObject(), cronTrigger.getObject());

            scheduler.start();
            log.debug("TCC:开启处理Zookeeper事件定时任务");

        } catch (Exception e) {
            log.error("TCC:处理Zookeeper事件定时任务发生异常！",e);
        }
    }





    @Override
    public void run(ApplicationArguments args) throws Exception {
        init();
    }
}

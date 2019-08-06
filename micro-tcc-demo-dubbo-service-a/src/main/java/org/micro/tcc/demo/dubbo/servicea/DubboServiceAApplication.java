package org.micro.tcc.demo.dubbo.servicea;
;
import org.micro.tcc.tc.component.EnableMicroTccTransaction;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Configuration;


/**
 *@author jeff.liu
 *@desc   描述
 *@date 2019/8/6
 */
@Configuration
@SpringBootApplication(/*scanBasePackages={"org.micro.tcc.**"}*/)
@EnableMicroTccTransaction
public class DubboServiceAApplication {

    public static void main(String[] args) {
        SpringApplication.run(DubboServiceAApplication.class, args);
    }

}


package org.micro.tcc.demo.serviceb;


import org.micro.tcc.tc.component.EnableMicroTccTransaction;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 *@author jeff.liu
 *@desc   描述
 *@date 2019/7/31
 */
@SpringBootApplication(/*scanBasePackages={"org.micro.tcc.**"}*/)
@EnableDiscoveryClient
@EnableMicroTccTransaction
public class SpringServiceBApplication {

    public static void main(String[] args) {
        SpringApplication.run(SpringServiceBApplication.class, args);

    }
}

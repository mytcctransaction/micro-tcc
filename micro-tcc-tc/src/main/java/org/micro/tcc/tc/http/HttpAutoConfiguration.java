package org.micro.tcc.tc.http;

import org.apache.http.HttpRequestInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Collections;

/**
 * @author jeff.liu
 *  HTTP 配置
 *  date 2019/8/6 10:26
 */
@Configuration
public class HttpAutoConfiguration {


    @Autowired
    private ConfigurableEnvironment configurableEnvironment;

}

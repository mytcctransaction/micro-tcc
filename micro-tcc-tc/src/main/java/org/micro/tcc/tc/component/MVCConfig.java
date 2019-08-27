package org.micro.tcc.tc.component;

import org.micro.tcc.tc.interceptor.WebControllerAspect;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

/**
*@author jeff.liu
*@desc   spring mvc 配置
*@date 2019/8/27
*/
@Configuration
public class MVCConfig extends WebMvcConfigurationSupport {
    @Bean
    public WebControllerAspect securityInterceptor() {
        return new WebControllerAspect();
    }

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(securityInterceptor()).excludePathPatterns("/static/*")
                .excludePathPatterns("/error").addPathPatterns("/**");
    }

}
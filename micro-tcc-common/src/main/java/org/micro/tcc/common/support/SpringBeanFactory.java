package org.micro.tcc.common.support;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 *@author jeff.liu
 *   描述
 * date 2019/7/31
 */
@Component
public class SpringBeanFactory implements BeanFactory, ApplicationContextAware {

    private ApplicationContext applicationContext;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        BeanFactoryBuilder.registerBeanFactory(this);
    }

    @Override
    public boolean isFactoryBean(Class clazz) {
        Map map = this.applicationContext.getBeansOfType(clazz);
        return map.size() > 0;
    }

    @Override
    public <T> T getBean(Class<T> val) {
        return this.applicationContext.getBean(val);
    }
}

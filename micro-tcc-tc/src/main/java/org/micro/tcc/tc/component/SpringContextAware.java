package org.micro.tcc.tc.component;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;

import java.util.Map;

/**
 * 资源文件读取工具
 *
 * @author jeff.liu
 *  date 2016年10月15日
 */
@Configuration
@Order(0)
public class SpringContextAware implements ApplicationContextAware {

	private static ApplicationContext context = null;

	public SpringContextAware() {
		super();
	}

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		context = applicationContext;
	}

	/**
	 * 根据名称获取bean
	 * @param beanName
	 *
	 */
	public static Object getBean(String beanName) {
		if(context==null)return null;
		return context.getBean(beanName);
	}

	/**
	 * 根据bean名称获取指定类型bean
	 * @param beanName bean名称
	 * @param clazz 返回的bean类型,若类型不匹配,将抛出异常
	 */
	public static <T> T getBean(String beanName, Class<T> clazz) {
		if(context==null)return null;
		return context.getBean(beanName, clazz);
	}
	/**
	 * 根据类型获取bean
	 * @param clazz
	 *
	 */
	public static <T> T getBean(Class<T> clazz) {
		if(context==null)return null;
		T t = null;
		Map<String, T> map = context.getBeansOfType(clazz);
		for (Map.Entry<String, T> entry : map.entrySet()) {
			t = entry.getValue();
		}
		return t;
	}

	/**
	 * 是否包含bean
	 * @param beanName
	 *
	 */
	public static boolean containsBean(String beanName) {
		if(context==null)return false;
		return context.containsBean(beanName);
	}

	/**
	 * 是否是单例
	 * @param beanName
	 *
	 */
	public static boolean isSingleton(String beanName) {
		return context.isSingleton(beanName);
	}

	/**
	 * bean的类型
	 * @param beanName
	 *
	 */
	public static Class getType(String beanName) {
		return context.getType(beanName);
	}

}

package org.micro.tcc.common.support;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 *@author jeff.liu
 *@desc   描述
 *@date 2019/7/31
 */
public final class BeanFactoryBuilder {


    private BeanFactoryBuilder() {

    }

    private static List<BeanFactory> beanFactories = new ArrayList<BeanFactory>();

    private static ConcurrentHashMap<Class, SingeltonFactory> classFactoryMap = new ConcurrentHashMap<Class, SingeltonFactory>();

    public static <T> SingeltonFactory<T> factoryBean(Class<T> clazz) {

        if (!classFactoryMap.containsKey(clazz)) {

            for (BeanFactory beanFactory : beanFactories) {
                if (beanFactory.isFactoryOf(clazz)) {
                    //从spring bean容器取出
                    classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz, beanFactory.getBean(clazz)));
                }
            }
            //如果spring 容器没有，则从classloader实例化一个对象
            if (!classFactoryMap.containsKey(clazz)) {
                classFactoryMap.putIfAbsent(clazz, new SingeltonFactory<T>(clazz));
            }
        }

        return classFactoryMap.get(clazz);
    }

    public static void registerBeanFactory(BeanFactory beanFactory) {
        beanFactories.add(beanFactory);
    }

    public static class SingeltonFactory<T> {

        private volatile T instance = null;

        private String className;

        public SingeltonFactory(Class<T> clazz, T instance) {
            this.className = clazz.getName();
            this.instance = instance;
        }

        public SingeltonFactory(Class<T> clazz) {
            this.className = clazz.getName();
        }

        public T getInstance() {
            //如果sping容器为null，则从classloader 实例化一个对象
            if (instance == null) {
                synchronized (SingeltonFactory.class) {
                    if (instance == null) {
                        try {
                            ClassLoader loader = Thread.currentThread().getContextClassLoader();

                            Class<?> clazz = loader.loadClass(className);

                            instance = (T) clazz.newInstance();
                        } catch (Exception e) {
                            throw new RuntimeException("Failed to create an instance of " + className, e);
                        }
                    }
                }
            }

            return instance;
        }

    }
}
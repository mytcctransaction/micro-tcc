package org.micro.tcc.tc.component;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

import java.util.ArrayList;
import java.util.List;

/**
*@author jeff.liu
*@desc   描述
*@date 2019/8/6
*/
public class MicroTccSpringSelector implements ImportSelector, BeanFactoryAware {
    private BeanFactory beanFactory;

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {
        importingClassMetadata.getAnnotationTypes().forEach(System.out::println);
        System.out.println(beanFactory);
        boolean isEnable = importingClassMetadata.getAnnotationAttributes(EnableMicroTccTransaction.class.getName()).isEmpty();

        List<String> importClasses = new ArrayList<>();
        if(isEnable){
            importClasses.add(MicroTccSpringConfig.class.getName());
        }

        return importClasses.toArray(new String[0]);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}

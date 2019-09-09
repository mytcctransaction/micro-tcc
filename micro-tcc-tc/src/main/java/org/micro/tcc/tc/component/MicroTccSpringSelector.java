package org.micro.tcc.tc.component;

import lombok.extern.slf4j.Slf4j;
import org.micro.tcc.tc.annotation.EnableMicroTccTransaction;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.annotation.ImportSelector;
import org.springframework.core.type.AnnotationMetadata;

import java.util.ArrayList;
import java.util.List;

/**
*@author jeff.liu
*  spring boot 启动管理
* date 2019/8/6
*/
@Slf4j
public class MicroTccSpringSelector implements ImportSelector, BeanFactoryAware {
    private BeanFactory beanFactory;

    @Override
    public String[] selectImports(AnnotationMetadata importingClassMetadata) {

        boolean isEnable = importingClassMetadata.getAnnotationAttributes(EnableMicroTccTransaction.class.getName()).isEmpty();

        List<String> importClasses = new ArrayList<>();
        if(isEnable){
            importClasses.add(MicroTccSpringConfig.class.getName());
            String microTcc="           _                      _           \n" +
                    " _ __ ___ (_) ___ _ __ ___       | |_ ___ ___ \n" +
                    "| '_ ` _ \\| |/ __| '__/ _ \\ _____| __/ __/ __|\n" +
                    "| | | | | | | (__| | | (_) |_____| || (_| (__ \n" +
                    "|_| |_| |_|_|\\___|_|  \\___/       \\__\\___\\___|\n" +
                    "** Boot Startup ** \n";
            System.out.println(microTcc);
        }

        return importClasses.toArray(new String[0]);
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}

package org.micro.tcc.tc.component;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;


/**
*@author jeff.liu
*@desc  启用tcc 分布式事务注解
*@date 2019/8/27
*/
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
@Import(MicroTccSpringSelector.class)
public @interface EnableMicroTccTransaction {
}
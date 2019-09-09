package org.micro.tcc.tc.annotation;

import org.micro.tcc.common.constant.Model;
import org.micro.tcc.common.constant.Propagation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


/**
*@author jeff.liu
*@desc   tcc 声明事务注解
*@date 2019/8/27
*/
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface TccTransaction {

    public Propagation propagation() default Propagation.REQUIRED;

    public String confirmMethod() default "";

    public String cancelMethod() default "";

    public Model model() default Model.AT;

    public boolean asyncConfirm() default false;

    public boolean asyncCancel() default false;

    Class<? extends Throwable>[] rollbackFor() default {};

}
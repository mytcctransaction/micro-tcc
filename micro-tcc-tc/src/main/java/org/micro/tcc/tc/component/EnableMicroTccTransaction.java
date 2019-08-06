package org.micro.tcc.tc.component;

import org.springframework.context.annotation.Import;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Documented
@Target(ElementType.TYPE)
@Import(MicroTccSpringSelector.class)
public @interface EnableMicroTccTransaction {
}
package com.atguigu.gmall.realtime.beans;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.FIELD)//指定注解标注位置
@Retention(RetentionPolicy.RUNTIME) //注解作用范围
public @interface TransientSink {
}

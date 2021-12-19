package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.beans.TransientSink;
import com.atguigu.gmall.realtime.beans.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class ClickHouseUtil {
    //获取SinkFunction
    public static <T>SinkFunction<T> getJdbcSink(String sql) {
        //类名属性 和数据表字段  顺序一一对应
        SinkFunction<T> sinkFunction = JdbcSink.<T>sink(
                sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement ps, T obj) throws SQLException {
                        //通过反射获取当前流中的对象都有哪些属性
                        Field[] fieldArr = obj.getClass().getDeclaredFields();

                        //对所有的属性进行遍历
                        int skipNum = 0;
                        for (int i = 0; i < fieldArr.length; i++) {
                            Field field = fieldArr[i];

                            //判断当前属性 是否需要写到ClickHouse中
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            if(transientSink != null){
                                skipNum++;
                                continue;
                            }

                            //设置私有属性的访问权限
                            field.setAccessible(true);
                            try {
                                //获取属性的值    正常:对象.属性名   反射： 属性对象.get(实体对象)
                                Object fieldValue = field.get(obj);
                                //将属性的值 赋值给对应的问号占位符  注意：在给问号占位符赋值的时候，索引是从1开始
                                ps.setObject(i + 1 - skipNum, fieldValue);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }

                        }
                    }
                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .withUrl(GmallConfig.CLICKHOUSE_URL)

                        .build()
        );
        return sinkFunction;
    }


}
//构造者设计模式 ： 由内部类的构造方法构造外部类对象
/*
class Student{
    String name;
    String stdNo;
    int age;

    public Student(String name, String stdNo, int age) {
        this.name = name;
        this.stdNo = stdNo;
        this.age = age;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setStdNo(String stdNo) {
        this.stdNo = stdNo;
    }

    public void setAge(int age) {
        this.age = age;
    }
    static class Builder{
        String name;
        String stdNo;
        int age;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setStdNo(String stdNo) {
            this.stdNo = stdNo;
            return this;
        }

        public Builder setAge(int age) {
            this.age = age;
            return this;
        }
        public Student build(){
            return new Student(name,stdNo,age);
        }
    }
}*/

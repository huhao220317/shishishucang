package com.atguigu.gmall.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCDC02_SQL {
    public static void main(String[] args) throws Exception {
        //TODO 1 基本环境准备
        //1.1 流环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(1);
        //1.3 设置表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //根据从mysql中读到的数据创建动态表
        tableEnv.executeSql("CREATE TABLE userInfo (\n" +
                " id INT NOT NULL,\n" +
                " name STRING,\n" +
                " age INT\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = 'hadoop101',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = 'root',\n" +
                " 'database-name' = 'gmall2021_realtime',\n" +
                " 'table-name' = 't_user'\n" +
                ")");
        tableEnv.executeSql("select * from userInfo").print();
        env.execute();
    }
}

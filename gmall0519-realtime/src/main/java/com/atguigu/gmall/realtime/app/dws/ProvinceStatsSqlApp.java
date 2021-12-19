package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.beans.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //TODO 0.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度
        env.setParallelism(4);
        /*
        //CK相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop202:8020/gmall/flink/checkpoint/ProvinceStatsSqlApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        String topic = "dwm_order_wide";
        String groupId = "province_data";
        tableEnv.executeSql("CREATE TABLE ORDER_WIDE (province_id BIGINT, " +
                "province_name STRING,province_area_code STRING" +
                ",province_iso_code STRING,province_3166_2_code STRING,order_id STRING, " +
                "split_total_amount DOUBLE,create_time STRING,rowtime AS TO_TIMESTAMP(create_time) ," +
                "WATERMARK FOR  rowtime  AS rowtime)" +
                " WITH (" + MyKafkaUtil.getKafkaDDL(topic, groupId) + ")");

        //TODO 4.从动态表中查询数据  进行分组、开窗、聚合计算
        Table provinceTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND), 'yyyy-MM-dd HH:mm:ss') edt," +
                "province_id," +
                "province_name," +
                "province_area_code area_code," +
                "province_iso_code iso_code," +
                "province_3166_2_code iso_3166_2," +
                "count(distinct order_id) order_count," +
                "sum(split_total_amount) order_amount," +
                "UNIX_TIMESTAMP()*1000 ts " +
                " from " +
                " order_wide " +
                " group by " +
                " TUMBLE(rowtime, INTERVAL '10' SECOND)," +
                " province_id,province_name,province_area_code,province_iso_code,province_3166_2_code");

        //TODO 5.将动态表转换为流
        DataStream<ProvinceStats> provinceStatsDS = tableEnv.toAppendStream(provinceTable, ProvinceStats.class);

        //TODO 6.将流中的数据写到CK中
        provinceStatsDS.print(">>>>>>");
        provinceStatsDS.addSink(
                ClickHouseUtil.<ProvinceStats>getJdbcSink("insert into  province_stats_0519  values(?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
        env.execute();
    }
}

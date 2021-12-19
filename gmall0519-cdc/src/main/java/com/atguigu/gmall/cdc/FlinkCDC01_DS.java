package com.atguigu.gmall.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC01_DS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/flinkcdc/ck"));
        System.setProperty("HADOOP_USER_NAME","huhao");
        //升级前：MySQLSource 升级后MySqlSource
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("gmall2021_realtime") // set captured database
                .tableList("gmall2021_realtime.t_user") // set captured table
                .username("root")
                .password("root")
                /*
                * initial 首次启动做全表查询（全表扫描），接下来从binlog最新位置开始读
                * earliest 不会全表扫描，从binlog最开始位置读取，但是小心不是所有的数据在binlog中都有记录
                * latest 首次启动不查询，从binlog开始读，对历史数据不处理
                * timestamp 从指定偏移量开始读，但是偏移量需要特定工具，因此一般不用
                * */
                .startupOptions(StartupOptions.initial())
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env.addSource(mysqlSource).print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}

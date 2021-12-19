package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.app.func.DimSink;
import com.atguigu.gmall.realtime.app.func.MyDebeziumDeserializationSchema;
import com.atguigu.gmall.realtime.app.func.TableProcessFunction;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
//动态分流
public class BaseDBApp_mySelf {
    public static void main(String[] args) throws Exception {
        //TODO  1.基本环境准备
        //1.1 流处理环境
        //1.2 设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO  2.检查点相关的设置
        env.enableCheckpointing(5*1000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //每三秒重启一次，重启三次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000));
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall/ck"));
        System.setProperty("HADOOP_USER_NAME","huhao");
        //TODO  3.从kafka主题中读取业务流数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        //TODO  4.对读取数据进行类型转换String->json对象方便处理
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(r -> JSONObject.parseObject(r));
        //TODO  5.对流中的数据进行简单的ETL
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {//为真保留
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                boolean flag = jsonObject.getString("table") != null
                        && jsonObject.getString("table").length() > 0
                        && jsonObject.getJSONObject("data") != null
                        && jsonObject.getString("data").length() > 3; // data对象属性

                return flag;
            }
        });

        //TODO  6.使用FlinkCDC读取配置表数据
        DebeziumSourceFunction<String> mysqlSource = MySQLSource.<String>builder()
                .hostname("hadoop101")
                .port(3306)
                .databaseList("gmall2021_realtime") // set captured database
                .tableList("gmall2021_realtime.table_process") // set captured table
                .username("root")
                .password("root")
                /*
                 * initial 首次启动做全表查询（全表扫描），接下来从binlog最新位置开始读
                 * earliest 不会全表扫描，从binlog最开始位置读取，但是小心不是所有的数据在binlog中都有记录
                 * latest 首次启动不查询，从binlog开始读，对历史数据不处理
                 * timestamp 从指定偏移量开始读，但是偏移量需要特定工具，因此一般不用
                 * */
                .startupOptions(StartupOptions.initial())
                .deserializer(new MyDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
                //将mysql数据封装成资源
        DataStreamSource<String> mySQLDS = env.addSource(mysqlSource);
        //定义保存mysql流的状态变量并广播出去，确保每个业务流数据都能与全部配置流数据进行匹配
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>(
                "mapStateDescriptor",
                String.class,
                TableProcess.class
        );
        //TODO  7.将配置流进行广播，并创建广播状态
        BroadcastStream<String> broadcastDS = mySQLDS.broadcast(mapStateDescriptor);


        //TODO  8.将业务流和配置流连接在一起 connect

        BroadcastConnectedStream<JSONObject, String> connectDS = filterDS.connect(broadcastDS);
        //TODO  9.动态分流  事实数据--主流    维度数据--输出流
        OutputTag<JSONObject> dimTag = new OutputTag<JSONObject>("dimTag") {};
        SingleOutputStreamOperator<JSONObject> realDS = connectDS.process(new TableProcessFunction(dimTag, mapStateDescriptor));
        DataStream<JSONObject> dimDS = realDS.getSideOutput(dimTag);
        realDS.print(">>>>>>>>>>>>>");
        dimDS.print("############");
        //TODO  10.维度数据写到HBase
        //Phoenix本身支持事务且本身支持幂等性
        dimDS.addSink(new DimSink());

        //TODO  11.事实数据写到kafka对应主题
        //因为要分别写入不同的kafka主题，因此要自己手动开发
        //此时要考虑 如何保证精准一致性

        realDS.addSink(
                MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                        String topic = jsonObject.getString("sink_table");
                        return new ProducerRecord<byte[], byte[]>(topic,jsonObject.getJSONObject("data").toJSONString().getBytes());
                    }
                })
                );
        env.execute();
    }
}

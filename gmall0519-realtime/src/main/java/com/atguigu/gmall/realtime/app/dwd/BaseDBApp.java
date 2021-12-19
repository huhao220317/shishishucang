package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * Desc：业务数据动态分流  超重点 亮点
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //TODO  1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO  2.检查点相关的设置
        //2.1 开启检查点
        env.enableCheckpointing(5*1000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超市时间
        env.getCheckpointConfig().setCheckpointTimeout(60*1000L);
        //2.3 取消JOB的时候，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall_DB/ck"));
        //2.6 设置操作Hadoop的用户
        System.setProperty("HADOOP_USER_NAME","huhao");
        //TODO  3.从kafka主题中读取业务流数据
        //3.1 声明消费的主题以及消费者组
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);

        //3.3 消费数据 并封装为流
        //JSON格式字符串
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        //TODO  4.对读取的业务数据进行类型转换 jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);
        //TODO  5.对流中的数据进行简单的ETL
        //过滤空和删除操作
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject jsonObject) throws Exception {
                boolean flag = jsonObject.getString("table") != null
                        && jsonObject.getString("table").length() > 0
                        && jsonObject.getJSONObject("data") != null
                        && jsonObject.getString("data").length() > 3; // data对象属性
                return flag;
            }
        });
        filterDS.print("filterDS>>>>>");
        //TODO  6.使用FlinkCDC读取配置表数据
        //TODO  7.将配置流进行广播，并创建广播状态
        //TODO  8.将业务流和配置流连接在一起 connect
        //TODO  9.动态分流  事实数据--主流    维度数据--输出流

        //TODO  10.维度数据写到HBase
        //TODO  11.事实数据写到kafka对应主题
        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.*;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.adaptors.PatternFlatSelectAdapter;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关配置
        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题和消费者组
        String topic = "dwd_page_log";
        String groupId = "user_jump_detail_app_group";

        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据封装为流
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);
        /*DataStream<String> kafkaDS = env
                .fromElements(
                        "{\"common\":{\"mid\":\"101\"},\"page\":{\"page_id\":\"home\"},\"ts\":10000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"home\"},\"ts\":12000}",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"home\"},\"ts\":15000} ",
                        "{\"common\":{\"mid\":\"102\"},\"page\":{\"page_id\":\"good_list\",\"last_page_id\":" +
                                "\"detail\"},\"ts\":30000} "
                );*/

        //TODO 4.对流中的数据类型进行转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(r -> JSONObject.parseObject(r));

        //jsonObjDS.print(">>>>>");

        //跳出的特征
        //1，last_page_id 为null
        //2.一定时间范围内 没有访问行为 这里是十秒，因为是对数据时间的判断，所以这里是使用事件时间

        //指定数据中的时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                        return element.getLong("ts");
                                    }
                                }
                        )
        );
        KeyedStream<JSONObject, String> keyedDS = jsonObjWithWatermarkDS.keyBy(r -> r.getJSONObject("common").getString("mid"));
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject event) {
                        String lastPageId = event.getJSONObject("page").getString("last_page_id");
                        if (lastPageId ==null || lastPageId.length()==0){
                            return true;
                        }
                        return false;
                    }
                }
        ).next("second").where(
                new SimpleCondition<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject subEvent) {
                        String pageId = subEvent.getJSONObject("page").getString("page_id");
                        if (pageId!=null && pageId.length()>0){
                            return true;
                        }
                        return false;
                        //within 在十秒内发生的事件进入second，没进入的被写入侧输出流
                        //接下来的处理中，侧流中的数据才是跳出数据
                        //return true;

                    }
                }
        ).within(Time.seconds(10));

        PatternStream<JSONObject> patternStream = CEP.pattern(keyedDS, pattern);

        OutputTag<String> lateDataOutputTag  = new OutputTag<String>("late_data") {};
        SingleOutputStreamOperator<String> timeDS = patternStream.flatSelect(lateDataOutputTag, new PatternFlatTimeoutFunction<JSONObject, String>() {
            @Override
            public void timeout(Map<String, List<JSONObject>> map, long l, Collector<String> collector) throws Exception {
                List<JSONObject> first = map.get("first");
                for (JSONObject jsonObject : first) {
                    collector.collect(jsonObject.toJSONString());
                }
            }
        }, new PatternFlatSelectFunction<JSONObject, String>() {
            @Override
            public void flatSelect(Map<String, List<JSONObject>> map, Collector<String> collector) throws Exception {
                List<JSONObject> first = map.get("first");
                for (JSONObject jsonObject : first) {
                    collector.collect( jsonObject.toJSONString() );
                }
            }
        });
        DataStream<String> timeOutDS = timeDS.getSideOutput(lateDataOutputTag);
        timeOutDS.print();
        timeOutDS.addSink(MyKafkaUtil.getKafkaSink("dwm_user_jump_detail"));
        env.execute();

    }
}

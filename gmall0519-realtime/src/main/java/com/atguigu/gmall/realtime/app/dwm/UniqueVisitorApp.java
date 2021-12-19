package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class UniqueVisitorApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 1 从Kafka中读取数据
        String topic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";

        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(topic, groupId));
        SingleOutputStreamOperator<JSONObject> dataJsonObj = kafkaDS.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String value) throws Exception {
                return JSONObject.parseObject(value);
            }
        });
        KeyedStream<JSONObject, String> keyedDS = dataJsonObj.keyBy(r -> r.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> filterDS = keyedDS.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                sdf = new SimpleDateFormat("yyyyMMdd");
                ValueStateDescriptor<String> valueStateDescriptor
                        = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(
                        Time.days(1)
                ).build());
                valueState = getRuntimeContext().getState(valueStateDescriptor);

            }

            @Override
            public boolean filter(JSONObject pageJson) throws Exception {
                String lastPageId = pageJson.getJSONObject("page").getString("last_page_id");
                //上页不为null 说明是从其他跳转 不属于访问
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }
                String lastVisitTime = valueState.value();
                String currentVisitTime = sdf.format(pageJson.getLong("ts"));
                if (lastVisitTime != null && lastVisitTime.length() > 0 && lastPageId.equals(currentVisitTime)) {
                    return false;
                } else {
                    valueState.update(lastVisitTime);
                    return true;
                }
            }
        });
        filterDS.print();
        filterDS
                .map(jsonObj -> jsonObj.toJSONString())
                .addSink(MyKafkaUtil.getKafkaSink(sinkTopic));
    env.execute();
    }
}

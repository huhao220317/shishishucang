package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.VisitorStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;


import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Date;

public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 基本环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //TODO 检查点CK的相关设置
        //TODO 从Kafka中读取数据
        //声明消费的主题以及消费者组
        String pvSourceTopic = "dwd_page_log";
        String uvSourceTopic = "dwm_unique_visit";
        String ujdSourceTopic = "dwm_user_jump_detail";
        String groupId = "visitor_stats_app";
        //创建消费者对象
        FlinkKafkaConsumer<String> pvSource = MyKafkaUtil.getKafkaSource(pvSourceTopic, groupId);
        FlinkKafkaConsumer<String> uvource = MyKafkaUtil.getKafkaSource(uvSourceTopic, groupId);
        FlinkKafkaConsumer<String> ujdSource = MyKafkaUtil.getKafkaSource(ujdSourceTopic, groupId);
        //消费数据封装为流

        DataStreamSource<String> pvStrDS = env.addSource(pvSource);
        DataStreamSource<String> uvStrDS = env.addSource(uvource);
        DataStreamSource<String> ujdStrDS = env.addSource(ujdSource);

        //pvStrDS.print(">>>");
        //uvStrDS.print("###");
        //ujdStrDS.print("$$$");
        //三条流进行数据类型转换
        SingleOutputStreamOperator<VisitorStats> pvStatsDS = pvStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                JSONObject common = jsonObject.getJSONObject("common");
                JSONObject page = jsonObject.getJSONObject("page");
                VisitorStats visitorStats = new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L,
                        1L,
                        0L,
                        0L,
                        page.getLong("during_time"),
                        jsonObject.getLong("ts")
                );
                String last_page_id = page.getString("last_page_id");
                if (last_page_id == null || last_page_id.length() == 0) {
                    visitorStats.setSv_ct(1L);
                }

                return visitorStats;
            }
        });
        SingleOutputStreamOperator<VisitorStats> uvStatsDS = uvStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                JSONObject common = jsonObject.getJSONObject("common");


                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        1L, 0l, 0l, 0l,
                        0l,
                        jsonObject.getLong("ts")
                );
            }
        });
        SingleOutputStreamOperator<VisitorStats> ujdStatDS = ujdStrDS.map(new MapFunction<String, VisitorStats>() {
            @Override
            public VisitorStats map(String jsonStr) throws Exception {
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                JSONObject common = jsonObject.getJSONObject("common");


                return new VisitorStats(
                        "",
                        "",
                        common.getString("vc"),
                        common.getString("ch"),
                        common.getString("ar"),
                        common.getString("is_new"),
                        0L, 0l, 0l, 1l,
                        0l,
                        jsonObject.getLong("ts")
                );
            }
        });
        // TODO 使用union将三条流的数据进行合并
        //connect 只能连接两个流 且数据类型可以不同
        //union 多流连接 但是类型需要统一
        DataStream<VisitorStats> unionDS = pvStatsDS.union(uvStatsDS, ujdStatDS);
        //设置水位线 官方建议靠近数据源，分流之前
        //但是此处是将多个流合成一条流，所以是合并之后设置水位线
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<VisitorStats>() {
                            @Override
                            public long extractTimestamp(VisitorStats element, long recordTimestamp) {
                                return element.getTs();
                            }
                        }));
        //TODO 分组 按照维度进行分组，如果按照mid进行分组，一个窗口内不会产生很好的聚合效果，因为这里一个mid代表是一个用户
        //一个用户的操作是很慢的
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> keyedDS = visitorStatsWithWatermarkDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {

                        return Tuple4.of(
                                value.getVc(),
                                value.getAr(),
                                value.getCh(),
                                value.getIs_new()
                        );
                    }
                }
        );

        //TODO 开窗
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );
        //TODO 聚合计算
        SingleOutputStreamOperator<VisitorStats> reduceDS = windowDS.reduce(new ReduceFunction<VisitorStats>() {
            @Override
            public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                return stats1;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                for (VisitorStats visitorStats : elements) {
                    visitorStats.setStt(DateTimeUtil.toYmdHms(new Date(context.window().getStart())));
                    visitorStats.setEdt(DateTimeUtil.toYmdHms(new Date(context.window().getEnd())));
                    visitorStats.setTs(System.currentTimeMillis());//获取当前处理时间，每个流都有自己的ts 最终以当前聚合时间为准
                    out.collect(visitorStats);
                }
            }
        });
        /*windowDS.aggregate(new AggregateFunction<VisitorStats, VisitorStats, VisitorStats>() {
            @Override
            public VisitorStats createAccumulator() {
                return null;
            }

            @Override
            public VisitorStats add(VisitorStats value, VisitorStats accumulator) {
                return null;
            }

            @Override
            public VisitorStats getResult(VisitorStats accumulator) {
                return null;
            }

            @Override
            public VisitorStats merge(VisitorStats a, VisitorStats b) {
                return null;
            }
        }, new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
            @Override
            public void process(Tuple4<String, String, String, String> stringStringStringStringTuple4, Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {

            }
        });*/
        reduceDS.print("**********");
        //TODO 将访客聚合统计结果保存到CK中
        // 补充：为什么控制台打印很多，但是clickhouse只有五条， 因为设置的withBatchSize 是攒批，是每个并行度攒够五条
        reduceDS.addSink(
                ClickHouseUtil.<VisitorStats>getJdbcSink("insert into visitor_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?)")
        );
        env.execute();
    }
}

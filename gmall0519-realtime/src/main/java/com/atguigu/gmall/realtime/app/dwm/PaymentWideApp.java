package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentInfo;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        /*
        //TODO 2.检查点相关设置
        //2.1 开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置取消job后，检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("xxx"));
        //2.6 指定操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        */
        String paymentInfoTopic = "dwd_payment_info";
        String orderWideTopic ="dwm_order_wide";
        String groupId = "payment_info_wide_app_group";
        //TODO 3.从kafka中读取数据
        //3.1 声明消费主题以及消费者组


        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> paymentInfoKafkaSource = MyKafkaUtil.getKafkaSource(paymentInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderWideKafkaSource = MyKafkaUtil.getKafkaSource(orderWideTopic, groupId);

        //3.3 消费数据  封装为流
        DataStreamSource<String> paymentInfoStrDS = env.addSource(paymentInfoKafkaSource);
        DataStreamSource<String> orderWideStrDS = env.addSource(orderWideKafkaSource);

        //TODO 4.对流中的数据类型进行转换   jsonStr->实体类对象
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoStrDS.map(
                new MapFunction<String, PaymentInfo>() {
                    @Override
                    public PaymentInfo map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr, PaymentInfo.class);
                    }
                }
        );

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideStrDS.map(
                new MapFunction<String, OrderWide>() {
                    @Override
                    public OrderWide map(String jsonStr) throws Exception {
                        return JSON.parseObject(jsonStr, OrderWide.class);
                    }
                }
        );

        //paymentInfoDS.print(">>>>");
        //orderWideDS.print("####");

        //TODO 5.指定Watermark以及提取事件时间字段
        //5.1 支付
        SingleOutputStreamOperator<PaymentInfo> paymentInfoWithWatermarkDS = paymentInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<PaymentInfo>() {
                                    @Override
                                    public long extractTimestamp(PaymentInfo paymentInfo, long recordTimestamp) {
                                        String callbackTime = paymentInfo.getCallback_time();
                                        return DateTimeUtil.toTs(callbackTime);
                                    }
                                }
                        )
        );
        //5.2 订单宽表
        SingleOutputStreamOperator<OrderWide> orderWideWithWatermarkDS = orderWideDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderWide>() {
                                    @Override
                                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                                        String createTime = orderWide.getCreate_time();
                                        return DateTimeUtil.toTs(createTime);
                                    }
                                }
                        )
        );

        //TODO 6.指定关联字段 订单id keyBy
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentInfoWithWatermarkDS.keyBy(PaymentInfo::getOrder_id);
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWatermarkDS.keyBy(OrderWide::getOrder_id);

        //TODO 7.使用intervalJoin进行连接
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS
                .intervalJoin(orderWideKeyedDS)
                .between(Time.seconds(-1800), Time.seconds(0))
                .process(
                        new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                            @Override
                            public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, Context ctx, Collector<PaymentWide> out) throws Exception {
                                out.collect(new PaymentWide(paymentInfo, orderWide));
                            }
                        }
                );

        //TODO 8.  将支付宽表数据写到kafka的dwm层
        paymentWideDS.print(">>>>");

        paymentWideDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_payment_wide"));

        env.execute();

    }
}

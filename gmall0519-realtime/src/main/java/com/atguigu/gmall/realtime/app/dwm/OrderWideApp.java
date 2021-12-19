package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.OrderDetail;
import com.atguigu.gmall.realtime.beans.OrderInfo;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.atguigu.gmall.realtime.utils.PhoenixUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //TODO 2.检查点相关的设置(略)

        //TODO 3.从Kafka中读取数据
        //3.1 声明消费的主题以及消费者组
        String orderInfoTopic = "dwd_order_info";
        String orderDetailTopic = "dwd_order_detail";

        String groupId = "order_wide_app_group";
        //3.2 创建消费者对象
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoTopic, groupId);
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(orderDetailTopic, groupId);

        //3.3 消费数据 封装为流
        DataStreamSource<String> orderInfoStrDS = env.addSource(orderInfoKafkaSource);
        DataStreamSource<String> orderDetailStrDS = env.addSource(orderDetailKafkaSource);

        //TODO 4.对流中的数据类型 进行转换  jsonStr->实体类对象
        //4.1 订单
        SingleOutputStreamOperator<OrderInfo> orderInfoDS = orderInfoStrDS.map(
                new RichMapFunction<String, OrderInfo>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String jsonStr) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(jsonStr, OrderInfo.class);
                        String createTime = orderInfo.getCreate_time();
                        orderInfo.setCreate_ts(sdf.parse(createTime).getTime());
                        return orderInfo;
                    }
                });

        //4.2 订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailDS = orderDetailStrDS.map(
                new RichMapFunction<String, OrderDetail>() {
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderDetail map(String jsonStr) throws Exception {
                        OrderDetail orderDetail = JSON.parseObject(jsonStr, OrderDetail.class);
                        String createTime = orderDetail.getCreate_time();
                        orderDetail.setCreate_ts(sdf.parse(createTime).getTime());
                        return orderDetail;
                    }
                }
        );

        //orderInfoDS.print(">>>>");
        //orderDetailDS.print("####");

        //TODO 5.指定Watermark以及提取事件时间字段
        //5.1 订单
        SingleOutputStreamOperator<OrderInfo> orderInfoWithWatermarkDS = orderInfoDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderInfo>() {
                                    @Override
                                    public long extractTimestamp(OrderInfo orderInfo, long recordTimestamp) {
                                        return orderInfo.getCreate_ts();
                                    }
                                }
                        )
        );
        //5.2 订单明细
        SingleOutputStreamOperator<OrderDetail> orderDetailWithWatermarkDS = orderDetailDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<OrderDetail>() {
                                    @Override
                                    public long extractTimestamp(OrderDetail orderDetail, long recordTimestamp) {
                                        return orderDetail.getCreate_ts();
                                    }
                                }
                        )
        );

        //TODO 6.指定关联字段订单id keyBy
        //6.1 订单
        KeyedStream<OrderInfo, Long> orderInfoKeyedDS = orderInfoWithWatermarkDS.keyBy(OrderInfo::getId);

        //6.2 订单明细
        KeyedStream<OrderDetail, Long> orderDetailKeyedDS = orderDetailWithWatermarkDS.keyBy(OrderDetail::getOrder_id);

        //TODO 7.使用intervalJoin进行连接
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderInfoKeyedDS
                .intervalJoin(orderDetailKeyedDS)
                .between(Time.seconds(-5), Time.seconds(5))
                .process(
                        new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                            @Override
                            public void processElement(OrderInfo orderInfo, OrderDetail orderDetail, Context ctx, Collector<OrderWide> out) throws Exception {
                                out.collect(new OrderWide(orderInfo, orderDetail));
                            }
                        }
                );

        //orderWideDS.print(">>>>>");

        //TODO 8.和用户维度进行关联
        //将异步I/O操作应用于DataStream作为 DataStream 的一次转换操作
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDS = AsyncDataStream.unorderedWait(
                orderWideDS,
                //实现分发请求的 AsyncFunction
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject userJsonObj) throws Exception {
                        String birthday = userJsonObj.getString("BIRTHDAY");
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        Date birthDayDate = sdf.parse(birthday);
                        Long birthDayTime = birthDayDate.getTime();
                        Long curTime = System.currentTimeMillis();
                        Long ageLong = (curTime - birthDayTime) / 1000 / 60 / 60 / 24 / 365;

                        orderWide.setUser_age(ageLong.intValue());
                        orderWide.setUser_gender(userJsonObj.getString("GENDER"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //orderWideWithUserDS.print(">>>>");

        //TODO 9.和地区维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDS = AsyncDataStream.unorderedWait(
                orderWideWithUserDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject provinceJsonObj) throws Exception {
                        orderWide.setProvince_name(provinceJsonObj.getString("NAME"));
                        orderWide.setProvince_area_code(provinceJsonObj.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(provinceJsonObj.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(provinceJsonObj.getString("ISO_3166_2"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //orderWideWithProvinceDS.print(">>>>");

        //TODO 10.和商品维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSkuDS = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDS,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject skuJsonObj) throws Exception {
                        orderWide.setSku_name(skuJsonObj.getString("SKU_NAME"));
                        orderWide.setCategory3_id(skuJsonObj.getLong("CATEGORY3_ID"));
                        orderWide.setSpu_id(skuJsonObj.getLong("SPU_ID"));
                        orderWide.setTm_id(skuJsonObj.getLong("TM_ID"));
                    }
                },
                60, TimeUnit.SECONDS
        );

        //orderWideWithSkuDS.print(">>>");

        //TODO 11.和SPU维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDS = AsyncDataStream.unorderedWait(
                orderWideWithSkuDS,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setSpu_name(jsonObject.getString("SPU_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getSpu_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 12.和类别维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithCategory3DS = AsyncDataStream.unorderedWait(
                orderWideWithSpuDS,
                new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setCategory3_name(jsonObject.getString("NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getCategory3_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 13.和品牌维度进行关联
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDS = AsyncDataStream.unorderedWait(
                orderWideWithCategory3DS, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);


        orderWideWithTmDS.print(">>>>");

        //TODO 14.  将订单宽表数据写到kafka的dwm层
        orderWideWithTmDS
                .map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink("dwm_order_wide"));

        env.execute();
    }
}

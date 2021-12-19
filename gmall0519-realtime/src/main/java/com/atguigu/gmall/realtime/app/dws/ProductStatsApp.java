package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.func.DimAsyncFunction;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.beans.PaymentWide;
import com.atguigu.gmall.realtime.beans.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
    //TODO 1.基本环境准备
    //1.1 指定流处理环境
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //1.2 设置并行度
        env.setParallelism(4);
    //TODO 2.检查点相关设置(略)
    //TODO 3.从Kafka中读取数据
    //3.1 声明消费的主题以及消费者组
    String groupId = "product_stats_app";
    String pageViewSourceTopic = "dwd_page_log";
    String orderWideSourceTopic = "dwm_order_wide";
    String paymentWideSourceTopic = "dwm_payment_wide";
    String cartInfoSourceTopic = "dwd_cart_info";
    String favorInfoSourceTopic = "dwd_favor_info";
    String refundInfoSourceTopic = "dwd_order_refund_info";
    String commentInfoSourceTopic = "dwd_comment_info";

    //3.2 创建消费者对象
    FlinkKafkaConsumer<String> pageViewSource  = MyKafkaUtil.getKafkaSource(pageViewSourceTopic,groupId);
    FlinkKafkaConsumer<String> orderWideSource  = MyKafkaUtil.getKafkaSource(orderWideSourceTopic,groupId);
    FlinkKafkaConsumer<String> paymentWideSource  = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic,groupId);
    FlinkKafkaConsumer<String> favorInfoSourceSouce  = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic,groupId);
    FlinkKafkaConsumer<String> cartInfoSource  = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic,groupId);
    FlinkKafkaConsumer<String> refundInfoSource  = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic,groupId);
    FlinkKafkaConsumer<String> commentInfoSource  = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic,groupId);

    //3.3 读取数据 封装为流
    DataStreamSource<String> pageLogStrDS = env.addSource(pageViewSource);
    DataStreamSource<String> favorInfoStrDS = env.addSource(favorInfoSourceSouce);
    DataStreamSource<String> cartInfoStrDS= env.addSource(cartInfoSource);
    DataStreamSource<String> orderWideStrDS= env.addSource(orderWideSource);
    DataStreamSource<String> paymentWideStrDS= env.addSource(paymentWideSource);
    DataStreamSource<String> refundInfoStrDS= env.addSource(refundInfoSource);
    DataStreamSource<String> commentInfoStrDS= env.addSource(commentInfoSource);

    //TODO 4.对读取的数据类型进行转换  jsonStr->ProductStats
    //4.1 转换点击以及曝光数据
    SingleOutputStreamOperator<ProductStats> clickAndDisplayStatsDS = pageLogStrDS.process(
            new ProcessFunction<String, ProductStats>() {
                @Override
                public void processElement(String jsonStr, Context ctx, Collector<ProductStats> out) throws Exception {
                    //将json字符串转换为json对象
                    JSONObject jsonObj = JSON.parseObject(jsonStr);
                    Long ts = jsonObj.getLong("ts");

                    //获取page属性对应的json对象
                    JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                    //获取当前页面的id
                    String pageId = pageJsonObj.getString("page_id");
                    //判断当前日志是否为点击行为
                    if("good_detail".equals(pageId)){
                        //获取点击的商品的id
                        Long skuId = pageJsonObj.getLong("item");
                        ProductStats productStats = ProductStats.builder()
                                .sku_id(skuId)
                                .click_ct(1L)
                                .ts(ts)
                                .build();
                        out.collect(productStats);
                    }

                    //判断是否为曝光行为
                    JSONArray displayArr = jsonObj.getJSONArray("displays");
                    if(displayArr != null && displayArr.size() > 0){
                        for (int i = 0; i < displayArr.size(); i++) {
                            JSONObject displayJsonObj = displayArr.getJSONObject(i);
                            //判断当前曝光的是不是商品
                            if("sku_id".equals(displayJsonObj.getString("item_type"))){
                                Long skuId = displayJsonObj.getLong("item");
                                ProductStats productStats = ProductStats.builder()
                                        .sku_id(skuId)
                                        .display_ct(1L)
                                        .ts(ts)
                                        .build();
                                out.collect(productStats);
                            }
                        }
                    }
                }
            }
    );
    //4.2 转换收藏数据
    SingleOutputStreamOperator<ProductStats> favorInfoStatsDS = favorInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject favorInfoJsonObj = JSON.parseObject(jsonStr);
                    Long skuId = favorInfoJsonObj.getLong("sku_id");
                    Long ts = DateTimeUtil.toTs(favorInfoJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(skuId)
                            .favor_ct(1L)
                            .ts(ts)
                            .build();
                }
            }
    );
    //4.3 转换加购数据
    SingleOutputStreamOperator<ProductStats> cartInfoStatsDS = cartInfoStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    JSONObject cartInfoJsonObj = JSON.parseObject(jsonStr);
                    Long skuId = cartInfoJsonObj.getLong("sku_id");
                    Long ts = DateTimeUtil.toTs(cartInfoJsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(skuId)
                            .cart_ct(1L)
                            .ts(ts)
                            .build();
                }
            }
    );
    //4.4 转换下单数据
    SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideStrDS.map(
            new MapFunction<String, ProductStats>() {
                @Override
                public ProductStats map(String jsonStr) throws Exception {
                    OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                    Long ts = DateTimeUtil.toTs(orderWide.getCreate_time());

                    return ProductStats.builder()
                            .sku_id(orderWide.getSku_id())
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount())
                            .ts(ts)
                            .build();
                }
            }
    );

    //4.5 转换支付流数据
    SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideStrDS.map(
            json -> {
                PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                Long ts = DateTimeUtil.toTs(paymentWide.getCallback_time());
                return ProductStats.builder().sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts).build();
            });

    //4.6 转换退单流数据
    SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoStrDS.map(
            json -> {
                JSONObject refundJsonObj = JSON.parseObject(json);
                Long ts = DateTimeUtil.toTs(refundJsonObj.getString("create_time"));
                ProductStats productStats = ProductStats.builder()
                        .sku_id(refundJsonObj.getLong("sku_id"))
                        .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(
                                new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                        .ts(ts).build();
                return productStats;

            });

    //4.7 转换评论流数据
    SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoStrDS.map(
            json -> {
                JSONObject commonJsonObj = JSON.parseObject(json);
                Long ts = DateTimeUtil.toTs(commonJsonObj.getString("create_time"));
                Long goodCt = GmallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ? 1L : 0L;
                ProductStats productStats = ProductStats.builder()
                        .sku_id(commonJsonObj.getLong("sku_id"))
                        .comment_ct(1L).good_comment_ct(goodCt).ts(ts).build();
                return productStats;
            });



    //TODO 5.将各个流的是数据合并在一起
    DataStream<ProductStats> unionDS = clickAndDisplayStatsDS.union(
            favorInfoStatsDS,
            cartInfoStatsDS,
            orderWideStatsDS,
            paymentStatsDS,
            refundStatsDS,
            commonInfoStatsDS
    );
    //TODO 6.指定Watermark以及提取事件时间字段
    SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
            WatermarkStrategy
                    .<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner(
                            new SerializableTimestampAssigner<ProductStats>() {
                                @Override
                                public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                    return productStats.getTs();
                                }
                            }
                    )
    );

    //TODO 7.分组
    KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(ProductStats::getSku_id);

    //TODO 8.开窗
    WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(TumblingEventTimeWindows.of(Time.seconds(10)));

    //TODO 9.聚合计算
    SingleOutputStreamOperator<ProductStats> productStatDS = windowDS.reduce(
            new ReduceFunction<ProductStats>() {
                @Override
                public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                    stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                    stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                    stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                    stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                    stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                    stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                    stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                    stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                    stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                    stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                    stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                    stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                    stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                    stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                    stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                    stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());

                    return stats1;

                }
            },
            new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                @Override
                public void process(Long aLong, Context context, Iterable<ProductStats> elements, Collector<ProductStats> out) throws Exception {
                    for (ProductStats productStats : elements) {
                        productStats.setStt(DateTimeUtil.toYmdHms(new Date(context.window().getStart())));
                        productStats.setEdt(DateTimeUtil.toYmdHms(new Date(context.window().getEnd())));
                        productStats.setTs(System.currentTimeMillis());
                        out.collect(productStats);
                    }
                }
            }
    );

    //TODO 10.对商品的维度进行关联
    //10.1 补充SKU维度
    SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(
            productStatDS,
            new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                @Override
                public String getKey(ProductStats productStats) {
                    return productStats.getSku_id().toString();
                }

                @Override
                public void join(ProductStats productStats, JSONObject dimJsonObj) throws Exception {
                    productStats.setSku_name(dimJsonObj.getString("SKU_NAME"));
                    productStats.setSku_price(dimJsonObj.getBigDecimal("PRICE"));
                    productStats.setCategory3_id(dimJsonObj.getLong("CATEGORY3_ID"));
                    productStats.setSpu_id(dimJsonObj.getLong("SPU_ID"));
                    productStats.setTm_id(dimJsonObj.getLong("TM_ID"));
                }
            },
            60, TimeUnit.SECONDS
    );

    //10.2 补充SPU维度
    SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS =
            AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                    new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {
                        @Override
                        public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                            productStats.setSpu_name(jsonObject.getString("SPU_NAME"));
                        }
                        @Override
                        public String getKey(ProductStats productStats) {
                            return String.valueOf(productStats.getSpu_id());
                        }
                    }, 60, TimeUnit.SECONDS);


    //10.3 补充品类维度
    SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS =
            AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                    new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {
                        @Override
                        public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                            productStats.setCategory3_name(jsonObject.getString("NAME"));
                        }
                        @Override
                        public String getKey(ProductStats productStats) {
                            return String.valueOf(productStats.getCategory3_id());
                        }
                    }, 60, TimeUnit.SECONDS);

    //10.4 补充品牌维度
    SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
            AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                    new DimAsyncFunction<ProductStats>("DIM_BASE_TRADEMARK") {
                        @Override
                        public void join(ProductStats productStats, JSONObject jsonObject) throws Exception {
                            productStats.setTm_name(jsonObject.getString("TM_NAME"));
                        }
                        @Override
                        public String getKey(ProductStats productStats) {
                            return String.valueOf(productStats.getTm_id());
                        }
                    }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print(">>>>>>");

    //TODO 11.将聚合计算的结果写到CK中
        productStatsWithTmDS.addSink(
                ClickHouseUtil.getJdbcSink("insert into product_stats_2021 values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
                );
        env.execute();
    }
}


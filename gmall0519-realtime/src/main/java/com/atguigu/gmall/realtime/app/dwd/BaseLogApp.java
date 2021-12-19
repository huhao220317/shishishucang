package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        //2.6 设置操作hdfs的用户
        System.setProperty("HADOOP_USER_NAME","huhao");
        //TODO 1，基本环境准备
        //1.1 设置流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //TODO 1.2 设置并行度
        env.setParallelism(4);//与kafka分区数一致
        //TODO 2 检查点相关设置
        //2，1 开启检查点 对状态进行备份，容错
        //分布式一致性算法 EXACTLY_ONCE：分界线对齐，AT_LEAST_ONCE：检查点不对齐
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setAlignmentTimeout(60*1000L);
        //2.3 设置将job取消时检查点是否保留
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 端到端精准一致性 kafka(手动维护偏移量) -> flink（检查点容错） ->kafka（ack=-1，幂等性，事务，两阶段提交 ）
        //设置每多少秒重启几次（重启策略）
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        //2.5 设置状态后端 内存（状态TaskManager 检查点JobManager） 文件系统（状态TaskManager 检查点指定的文件系统） RocksDB(状态TaskManager 检查点：jobManager，文件系统)
        env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/gmall/ck"));
        //TODO 3，从kafka中读取数据
        DataStreamSource<String> kafkaStreamSource =env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_group"));
        //TODO 4，对读取数据进行类型转换String->json对象方便处理
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStreamSource.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {

                return JSONObject.parseObject(s);
            }
        });
        //TODO 5，对新老访客进行修复（扩展）
        //TODO 5.1 根据mid进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDS.keyBy(r -> r.getJSONObject("common").getString("mid"));
        //TODO 5.2 修复状态
        SingleOutputStreamOperator<JSONObject> jsonObjWithIsNewDS = keyedDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> valueState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>(
                        "lastVisitDateState",
                        String.class
                ));
                sdf = new SimpleDateFormat("yyyyMMdd");
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                //获取用户访问标记
                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                //如果新老访客标记位为1的话，才有可能需要进行修复；如果为0，没有必要进行修复
                if ("1".equals(isNew)) {
                    //从状态中获取上次访问日期
                    String lastdate = valueState.value();
                    //获取当前访问日期
                    Long ts = jsonObject.getLong("ts");
                    String curVisitDate = sdf.format(ts);
                    if (lastdate != null && lastdate.length() > 1) {
                        //如果状态中的上次访问日期不为空，那么说明曾经访问过，需要对新老访客的标记进行修复
                        if (!curVisitDate.equals(lastdate)) {
                            isNew = "0";
                            jsonObject.getJSONObject("common").put("is_new", isNew);
                        }
                    } else {
                        //如果状态中的上次访问日期为空，那么说明当前设备还没有访问过
                        //将当前访问的日期  放到状态中
                        valueState.update(curVisitDate);
                    }
                    //获取当前访问日期
                }


                return jsonObject;
            }
        });
        //TODO 6，分流->侧输出流  启动日志->侧 曝光日志->侧 页面日志->主
        //6.1 定义侧输出流标签
        OutputTag<String> startTag = new OutputTag<String>("startTag") {};
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
        //6.2 分流
        SingleOutputStreamOperator<String> pageDS = jsonObjWithIsNewDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, Context ctx, Collector<String> out) throws Exception {
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                String jsonStr = jsonObject.toJSONString();
                if (startJsonObj != null && startJsonObj.size() > 0) {
                    //如果启动属性不为空  说明是启动日志  放到启动侧输出流中
                    ctx.output(startTag, jsonStr);
                } else {
                    //如果不是启动日志，那么都属于页面日志,将页面日志放到主流中
                    out.collect(jsonStr);

                    //判断是否是曝光日志
                    JSONArray displaysArr = jsonObject.getJSONArray("displays");
                    if (displaysArr != null && displaysArr.size() > 0) {
                        //获取时间和页面数据 方便后续查询：曝光的时哪个页面 哪个时间
                        String page_id = jsonObject.getJSONObject("page").getString("page_id");
                        Long ts = jsonObject.getLong("ts");
                        for (int i = 0; i < displaysArr.size(); i++) {
                            JSONObject jsonObject1 = displaysArr.getJSONObject(i);
                            jsonObject1.put("page_id", page_id);
                            jsonObject1.put("ts", ts);
                            ctx.output(displayTag, jsonObject1.toJSONString());
                        }
                    }
                }
            }
        });
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        startDS.print(">>>");
        displayDS.print("###");
        pageDS.print("$$$$");
        //TODO 7，将不同流的数据写入kafka
        startDS.addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));
        pageDS.addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        env.execute();
    }
}

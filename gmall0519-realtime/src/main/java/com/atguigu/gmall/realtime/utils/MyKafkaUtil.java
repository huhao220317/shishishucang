package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.omg.CORBA.TRANSACTION_MODE;

import javax.annotation.Nullable;
import java.util.Properties;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER="hadoop101:9092,hadoop102:9092,hadoop103:9092";
    private static final String DEFAULT_TOPIC="default_topic";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }
    //获取kafka的生产者对象
    /*public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        return new FlinkKafkaProducer<String>(KAFKA_SERVER,topic,new SimpleStringSchema());
    }*/
    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "");
        //生产者向kafka发送数据，时要保证消费者精准一致性 要做三件事 1，ack=-1 2.开启幂等性 3，通过事务控制
        return new FlinkKafkaProducer<String>(DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String jsonStr, @Nullable Long aLong) {

                return new ProducerRecord<byte[], byte[]>(topic,jsonStr.getBytes());
            }
        }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    public static <T>SinkFunction<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,KAFKA_SERVER);
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "");
        //生产者向kafka发送数据，时要保证消费者精准一致性 要做三件事 1，ack=-1 2.开启幂等性 3，通过事务控制
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,kafkaSerializationSchema,props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
    public static String getKafkaDDL(String topic,String groupId){
        String ddl="'connector' = 'kafka', " +
                " 'topic' = '"+topic+"',"   +
                " 'properties.bootstrap.servers' = '"+ KAFKA_SERVER +"', " +
                " 'properties.group.id' = '"+groupId+ "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'  ";
        return  ddl;
    }

}

package com.atguigu.gmall.cdc;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class FlinkCDC03_Custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //升级前：MySQLSource 升级后MySqlSource
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
                .deserializer(new MyStringDebezium()) // converts SourceRecord to JSON String
                .build();
        // enable checkpoint
        env.enableCheckpointing(3000);

        env.addSource(mysqlSource).print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
    public static class MyStringDebezium implements DebeziumDeserializationSchema<String> {

        @Override
        public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {
            Struct valueStruct = (Struct)sourceRecord.value();
            Struct sourceStruct = valueStruct.getStruct("source");
            //获取数据库名
            String database = sourceStruct.getString("db");
            //获取表名
            String table = sourceStruct.getString("table");
            //获取操作类型
            String type = Envelope.operationFor(sourceRecord).toString().toLowerCase();
            if("create".equals(type)){
                type = "insert";
            }

            //获取影响的记录
            JSONObject dataJsonObj = new JSONObject();
            Struct afterStruct = valueStruct.getStruct("after");
            if(afterStruct != null){
                List<Field> fieldList = afterStruct.schema().fields();
                //对结构体中所有的属性进行遍历
                for (Field field : fieldList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(field);
                    dataJsonObj.put(fieldName,fieldValue);
                }
            }

            JSONObject resJsonObj = new JSONObject();
            resJsonObj.put("database",database);
            resJsonObj.put("table",table);
            resJsonObj.put("type",type);
            resJsonObj.put("data",dataJsonObj);

            collector.collect(resJsonObj.toJSONString());

        }

        @Override
        public TypeInformation getProducedType() {
            return TypeInformation.of(String.class);
        }
    }
}

package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * 处理广播流数据
 *  从流中读取配置信息
 *  将配置信息封装为TableProcess对象
 *  获取广播状态
 *  将配置信息放到状态中
 *      key:sourceTable + ":" + operatorType
 *      value:TableProcess对象
 * 处理业务数据
 *  获取广播状态
 *  从处理的业务流中获取业务数据库名和操作类型封装为key：sourceTable + ":" + operatorType
 *  从广播状态中获取当前处理的这条数据对应的配置信息
 *  根据配置信息判断是事实还是维度  维度——>维度侧输出流   事实——>主流
 *
 * 处理广播流中的配置信息时，提前创建维度表
 * 处理业务流数据时 处理字段的过滤
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {
    private MapStateDescriptor<String, TableProcess> mapStateDescriptor;
    private OutputTag<JSONObject> dimTag;
    private Connection connection ;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //1，注册驱动
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        //2，获取连接
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

    }

    public TableProcessFunction(OutputTag<JSONObject> dimTag, MapStateDescriptor<String, TableProcess> mapStateDescriptor) {
        this.mapStateDescriptor = mapStateDescriptor;
        this.dimTag = dimTag;
    }

    @Override
    public void processElement(JSONObject jsonObj, ReadOnlyContext readOnlyContext, Collector<JSONObject> collector) throws Exception {
        //获取广播状态
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);

        //获取当前处理的业务数据
        String table = jsonObj.getString("table");
        String type = jsonObj.getString("type");
        if (type.equals("bootstrap-insert")){
            type = "insert";
            jsonObj.put("type",type);
        }
        String key = table + ":" + type;
        //根据key 从广播状态中获取当前处理的这条数据 对应的配置信息
        TableProcess tableProcess = broadcastState.get(key);
        if (tableProcess!=null){
            //在配置表中找到了对应的配置

            //不管是事实还是维度 在向下传递时 都应该获取目的地
            String sinkTable = tableProcess.getSinkTable();
            jsonObj.put("sink_table",sinkTable);

            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            String sinkColumns = tableProcess.getSinkColumns();
            //将不用的字段进行过滤
            filterColume(dataJsonObj,sinkColumns);

            String sinkType = tableProcess.getSinkType();
            System.out.println(key);
            if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)){
                //是维度数据  --放到维度侧输出流中
                readOnlyContext.output(dimTag,jsonObj);

            }else if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)){
                //是事实数据 --放主流中
                collector.collect(jsonObj);
            }

        }else {
            //当前处理的这条业务数据 在配置表中没有找到对应的配置
            System.out.println("no this key in tableProcess :" + key);
        }
    }

    private void filterColume(JSONObject dataJsonObj, String sinkColumns) {
        //获取保留的字段
        String[] fieldArr = sinkColumns.split(",");
        //为了判断字段的包含关系  将数组转换为集合
        List<String> fieldList = Arrays.asList(fieldArr);

        //遍历json对象中的元素
        Set<Map.Entry<String, Object>> entrySet = dataJsonObj.entrySet();

        entrySet.removeIf(entry->!fieldList.contains(entry.getKey()));


    }

    @Override
    public void processBroadcastElement(String o, Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject jsonObj = JSON.parseObject(o);
        TableProcess tableProcess = JSON.parseObject(jsonObj.getString("data"), TableProcess.class);

        String sourceTable = tableProcess.getSourceTable();
        String operateType = tableProcess.getOperateType();
        //kafka 事实     HBase  维度
        String sinkType = tableProcess.getSinkType();

        String sinkTable = tableProcess.getSinkTable();

        String sinkPk = tableProcess.getSinkPk();
        //建表字段，保留字段
        String sinkColumns = tableProcess.getSinkColumns();
        //建表扩展语句
        String sinkExtend = tableProcess.getSinkExtend();

        //判断当前配置表数据是维度表还是事实表
        if (TableProcess.SINK_TYPE_HBASE.equals(sinkType) && "insert".equals(operateType)){
            //事维度配置 先创建维度表 建表要素 表名 列名 主键 扩展语句

            checkTable(sinkTable,sinkColumns,sinkPk,sinkExtend);
        }
        //拼接广播状态的key
        String key = sourceTable + ":" + operateType;
        BroadcastState<String, TableProcess> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(key,tableProcess);
    }
    //创建维度表
    private void checkTable(String tableName, String fields, String pk, String ext) {
        if (pk == null ){
            pk = "id";
        }
        if (ext == null){
            ext = ""; //防止后续SQL拼接出问题
        }
        //拼接建表语句
        String[] fieldArr = fields.split(",");
        StringBuilder createSql = new StringBuilder("create table if not exists " + GmallConfig.HBASE_SCHEMA + "." + tableName + "(");
        for (int i = 0; i < fieldArr.length; i++) {
            String fieldName = fieldArr[i];
            if(fieldName.equals(pk)){
                createSql.append(fieldName +" varchar primary key ");
            }else{
                createSql.append(fieldName +" varchar ");
            }
            //判断是不是最后一个字段
            if(i < fieldArr.length -1){
                createSql.append(",");
            }
        }
        createSql.append(")" + ext);
        System.out.println("在phoenix的建表语句 ： " + createSql.toString());
        PreparedStatement ps =null;
        try {
             ps = connection.prepareStatement(createSql.toString());
            ps.execute();
        } catch (Exception throwables) {
            new RuntimeException("在Phoenix中建表失败！！！！");
        }finally {
            //释放资源
            if (ps!=null){
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }


    }
}

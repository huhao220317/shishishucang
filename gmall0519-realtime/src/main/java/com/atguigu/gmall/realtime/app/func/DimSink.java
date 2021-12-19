package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.RedisUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.Set;

/**
 * 将维度数据写入到Phoenix
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection connection;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) throws Exception {
        PreparedStatement ps = null;
        //获取要插入的维度表
        String sinktable = jsonObj.getString("sink_table");
        JSONObject sinkId = jsonObj.getJSONObject("data");
        try {
            //获取要插入的维度表
            //String sinktable = jsonObj.getString("sink_table");
            //获取插入的数据
            JSONObject dataJsonObj = jsonObj.getJSONObject("data");
            String upsertSql = genUpsertSql(sinktable,dataJsonObj);

            ps = connection.prepareStatement(upsertSql);
            ps.executeUpdate();
            //使用JDBC向phoenix表中插入数据需要手动提交事务
            connection.commit();
        }catch (Exception e){
            e.printStackTrace();
            throw new RuntimeException("向维度表插入数据失败");
        }finally {
            if (ps!=null){
                ps.close();
            }
        }
       //如果维度表数据被修改，那么情况Redis缓存的维度数据
        if ("update".equals(jsonObj.getString("type"))){
            DimUtil.deleteCached(sinktable,sinkId.getString("id"));
        }
        //拼接SQL

    }

    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {

        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        String upsertSql = "upsert into "+GmallConfig.HBASE_SCHEMA+"."
                +tableName+" ("+ StringUtils.join(keys,",") +") " +
                " values('"+StringUtils.join(values,"','")+"')";
        return upsertSql;
    }
}

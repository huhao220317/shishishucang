package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * 用户维度查询的工具类，统一做成JSON形式
 */
public class DimUtil {
    public static JSONObject getDimInfo(String tableName , String Id){
       return getDimInfo(tableName,Tuple2.of("ID",Id));
    }
    //加入旁路缓存
    //选型 redis：value类型：String  TTL（过期时间）：1day  key：dim:维度表名:主键值1_主键值2
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... colNameAndValues){
        StringBuilder selectDimSql = new StringBuilder("select * from " + tableName + " where ");
        //拼接查询redis的key
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");

        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;
            selectDimSql.append(colName + "='" + colValue + "'");
            redisKey.append(colValue);

            if (i < colNameAndValues.length - 1) {
                selectDimSql.append(" and ");
                redisKey.append("_");
            }
        }
        Jedis jedis = null;
        String jsonStr = null;
        JSONObject dimJsonObj = null;
        try {
            jedis = RedisUtil.getJedis();
            jsonStr = jedis.get(redisKey.toString());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("从Redis中查询缓存维度数据失败");
        }

        if (jsonStr!=null && jsonStr.length()>0){
            //命中缓存
            dimJsonObj =  JSONObject.parseObject(jsonStr);
        }else {
            System.out.println("查询维度的SQL：" + selectDimSql);

            //底层还是调用PhoenixUtil
            List<JSONObject> dimJsonObjList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);


            if (dimJsonObjList!=null && dimJsonObjList.size()>0){
                //因为是根据主键查询维度，所以只有一个维度
                dimJsonObj = dimJsonObjList.get(0);
                if (jedis!=null){
                    jedis.setex(redisKey.toString(),24*3600,dimJsonObj.toString());
                }
            }else {
                System.out.println("维度没有找到！ 执行的SQL是："+selectDimSql);
            }

            //

        }
        if (jedis!=null){
            jedis.close();
            System.out.println("将redis连接放入连接池");
        }

        return dimJsonObj;

    }
    //查询参数和个数不唯一
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... colNameAndValues){
        StringBuilder selectDimSql = new StringBuilder("select * from " + tableName + " where ");

        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");

        for (int i = 0; i < colNameAndValues.length; i++) {
            Tuple2<String, String> colNameAndValue = colNameAndValues[i];
            String colName = colNameAndValue.f0;
            String colValue = colNameAndValue.f1;
            selectDimSql.append(colName + "='" + colValue + "'");
            redisKey.append(colValue);

            if (i < colNameAndValues.length - 1) {
                selectDimSql.append(" and ");
                redisKey.append("_");
            }
        }
        System.out.println("查询维度的SQL：" + selectDimSql);

        //底层还是调用PhoenixUtil
        List<JSONObject> dimJsonObjList = PhoenixUtil.queryList(selectDimSql.toString(), JSONObject.class);
        JSONObject dimJsonObj = null;

        if (dimJsonObjList!=null && dimJsonObjList.size()>0){
            //因为是根据主键查询维度，所以只有一个维度
            dimJsonObj = dimJsonObjList.get(0);
        }else {
            System.out.println("维度没有找到！ 执行的SQL是："+selectDimSql);
        }

        //
        return dimJsonObj;

    }
    //根据Key从redis中删除掉缓存的维度数据
    //先更新再删除：
    public static void deleteCached(String tableName,String id){
        String redisKey = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            jedis.del(redisKey);
            jedis.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        JSONObject dimInfoNoCache =getDimInfo("DIM_BASE_TRADEMARK", "26");
        System.out.println(dimInfoNoCache.toString());
    }
}

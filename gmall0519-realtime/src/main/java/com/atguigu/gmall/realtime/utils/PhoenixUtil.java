package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 胡浩
 * @date 从phoenix中查询维度数据
 */
public class PhoenixUtil {

    private static Connection conn;

    private static void initConnection() throws Exception {
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        //设置schema
        conn.setSchema("GMALL2021_REALTIME");
    }

    /**
     * @param sql
     * @param type
     * @param <T>
     * @return ORM 对象关系映射
     * T1 声明一个泛型模板  T2当前集合中的返回值类型
     */
    public static <T> List<T> queryList(String sql, Class<T> type) {
        List<T> resList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (conn == null) {
                initConnection();
            }
            ps = conn.prepareStatement(sql);
            rs = ps.executeQuery();
            //获取查询结果集的元数据信息 通过元数据信息获取列的相关内容
            ResultSetMetaData metaData = rs.getMetaData();

            while (rs.next()) {
                //通过传递的参数，创建对应类型的对象，用于接受查询结果集
                T obj = type.newInstance();
                //获取列名，将列名作为对象的属性名，给对象的属性赋值
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String columnName = metaData.getColumnName(i);
                    Object columnValue = rs.getObject(i);
                    BeanUtils.setProperty(obj, columnName, columnValue);
                }
                //rs.getObject(1);从第一列开始获取

                //将查询结果集封装的对象放入list集合中
                resList.add(obj);
            }
        } catch (Exception e) {
            throw new RuntimeException("从Phoenix表中查询数据失败!!! 执行的SQL是：" + sql);

        } finally {
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException s) {
                    s.printStackTrace();

                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException s) {
                    s.printStackTrace();
                }
            }
        }
        return resList;
    }

    public static void main(String[] args) {
        List<JSONObject> jsonObjects = queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class);
        System.out.println(jsonObjects);

    }
}

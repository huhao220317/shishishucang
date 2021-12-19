package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

public interface DimJoinFunction<T> {
    String getKey(T obj);
    void join(T obj, JSONObject dimJsonObj) throws Exception;
}

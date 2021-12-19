package com.atguigu.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.OrderWide;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutorService;

//泛型模板 静态方法：在返回值前声明。 定义类：在类的后面
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T>{
    private ExecutorService executorService;
    private String tableName;
    public DimAsyncFunction(String tableName){
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = ThreadPoolUtil.getInstance();
    }

    //obj : 流中处理的数据
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //从线程池中获取线程对象,并提交,相当于运行的star，执行的时run方法
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            long start = System.currentTimeMillis();
                            //run方法中做的事，就是异步进行维度关联的代码
                            //通过事实对象获取维度主键
                            String key = getKey(obj);
                            //根据维度主键获取维度信息
                            JSONObject dimJsonObj = DimUtil.getDimInfo(tableName, key);
                            //将维度信息补充到对象obj的相关属性上
                            join(obj,dimJsonObj);
                            long end = System.currentTimeMillis();
                            System.out.println("异步查询耗时：" + (end - start) + "毫秒");
                            //将异步响应结果向下游传递
                            resultFuture.complete(Collections.singleton(obj));
                        }catch (Exception e){
                            e.printStackTrace();
                        }

                    }
                });

    }



}

package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;
    private ThreadPoolUtil(){}
    public static ThreadPoolExecutor getInstance(){
        if (pool == null){ //是为了判断当前对象是否为null，如果不为null就不执行创建对象操作
            synchronized (ThreadPoolUtil.class){
                if (pool == null){//判断并发场景下，是不是已经有线程在之前已经进来完成对象创建
                    System.out.println("----创建线程池----");
                    //线程池初始线程数，线程池最大线程数，空闲线程数量（超出这个数量时，对空闲线程进行销毁），单位秒
                    //队列：任务进来如果当前线程池没有线程，任务先进入等待队列
                    pool = new ThreadPoolExecutor(4,20,300, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(Integer.MAX_VALUE));
                }
            }
        }
        return pool;
    }
}

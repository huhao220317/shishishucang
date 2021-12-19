package com.atguigu.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool;
    public static Jedis getJedis(){
        if (jedisPool == null){
            initJedisPool();
        }
        System.out.println("----获取Jedis连接-----");
        Jedis jedis = jedisPool.getResource();

        return jedis;
    }

    private static void initJedisPool() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(100);//最大连接数
        jedisPoolConfig.setMaxIdle(5); //最大空闲连接
        jedisPoolConfig.setMinIdle(5); //最小空闲数
        jedisPoolConfig.setBlockWhenExhausted(true);//资源等待
        jedisPoolConfig.setMaxWaitMillis(2 * 1000L);//最大等待时间
        jedisPoolConfig.setTestOnBorrow(true);//测试连接是否畅通
        jedisPool = new JedisPool(jedisPoolConfig,"hadoop101",6379,10000);
    }

    public static void main(String[] args) {
        System.out.println(getJedis().ping());
    }
}

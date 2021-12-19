package com.atguigu.gmall.realtime.bean;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @ClassName gmall-flink-RedisUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日11:57 - 周六
 * @Describe
 */
public class RedisUtil {
    /*
     * Explain
     *  static简称类变量,存放在方法区，属于类变量，被所有实例所共享
     *  生命周期和类一样,类销毁的时候,静态变量才被销毁
     * */
    public static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        //若连接池不为空直接取出
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(100);//最大可用连续数
            jedisPoolConfig.setBlockWhenExhausted(true); //连接耗尽是否等待
            jedisPoolConfig.setMaxWaitMillis(2000); //等待时间
            jedisPoolConfig.setMaxIdle(5); //最大闲置连接数
            jedisPoolConfig.setMinIdle(5); //最小闲置连接数
            //jedisPoolConfig.setTestOnBorrow(true); //取连接的时候进行一下测试 ping pong

            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102", 6379);

            System.out.println("开辟连接池");
        } else {
            System.out.println("连接池:" + jedisPool.getNumActive());
        }

        return jedisPool.getResource();//从连接池中取出jedis客户端
    }

}

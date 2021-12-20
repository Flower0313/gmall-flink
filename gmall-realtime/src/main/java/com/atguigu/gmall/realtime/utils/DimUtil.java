package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.RedisUtil;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

import static com.atguigu.gmall.realtime.bean.RedisUtil.jedisPool;
import static com.atguigu.gmall.realtime.common.CommonEnv.REDIS_PASSWORD;
import static com.atguigu.gmall.realtime.common.GmallConfig.HBASE_SCHEMA;
import static com.atguigu.gmall.realtime.utils.PhoenixUtil.conn;

/**
 * @ClassName gmall-flink-DimUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日10:30 - 周六
 * @Describe 将jdbc中查询出的数据, 将数据转换为Json格式,与PhoenixUtil搭配使用
 */
public class DimUtil {
    /**
     * @param tableName    表名,类似DIM_USER_INFO
     * @param columnValues Tuple2<列名,列值>
     */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... columnValues) throws Exception {
        /*
         * Explain
         * 输入表名+<列名,列值> => 将查询数据以json格式返回
         * {"ID":"1001","NAME":"fucker","AGE":"9"}
         * */
        if (columnValues.length <= 0) {
            throw new RuntimeException(">>>至少查询一个维度条件!");
        }
        //Step-1 查询Phoenix之前先查询Redis是否有该数据的缓存
        Jedis jedis = RedisUtil.getJedis();
        jedis.auth(REDIS_PASSWORD);
        //columnValues[0].f1为id字段值
        String redisKey = tableName + ":" + columnValues[0].f1;
        String dimInfoJsonStr = jedis.get(redisKey);
        if (dimInfoJsonStr != null && dimInfoJsonStr.length() > 0) {
            System.out.println("Redis中含有" + redisKey + "缓存数据");
            //重置过期时间
            jedis.expire(redisKey, 60 * 60);
            jedis.close();
            //若redis中有此数据的缓存,直接取出返回,不用执行下面的步骤了
            return JSONObject.parseObject(dimInfoJsonStr);
        }

        //Step-2 Redis中没查询到,那就从Phoenix中查询
        //Attention 拼接where条件
        StringBuilder whereSql = new StringBuilder(" where ");
        //TODO 遍历一行中每个字段和其字段值
        for (int i = 0; i < columnValues.length; i++) {
            //获取单个查询条件
            Tuple2<String, String> columnValue = columnValues[i];
            String column = columnValue.f0;//列名
            String value = columnValue.f1;//列值
            whereSql.append(column).append("='").append(value).append("'");
            //如果不是最后一个条件,那就叫and连接多个条件
            if (i < columnValues.length - 1) {
                whereSql.append(" and ");
            }
        }

        //TODO 拼接sql
        String querySql = "select * from " + HBASE_SCHEMA + "." + tableName + whereSql.toString();
        System.out.println(">>>查询维度表sql语句:" + querySql);

        //TODO 从查询Phoenix中维度数据
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(querySql, JSONObject.class, false);

        //Step-3 返回查询结果集中第一条元素,因为也只会查询出一条语句
        JSONObject dimJsonObj = jsonObjects.get(0);
        //TODO 先将此数据存入redis缓存
        jedis.set(redisKey, dimJsonObj.toString());
        jedis.expire(redisKey, 60 * 60);//设置过期时间
        jedis.close();

        //System.out.println(">>>" + jsonObjects);
        return dimJsonObj;
    }


    /**
     * 间接调用getDimInfo方法,将Tuple2<>的第一个参数写死为id,且个数只有一个
     *
     * @param tableName 表名
     * @param value     列值,注意这里将列名写死了, where id='value'
     * @throws Exception
     */
    public static JSONObject getDimInfo(String tableName, String value) throws Exception {
        return getDimInfo(tableName, new Tuple2<>("id", value));
    }

    /**
     * 当hbase中的维度表在update的时候调用此方法,以删除Redis中缓存
     * 防止数据不对称
     *
     * @param tableName 表名(DIM_BASE_PROVINCE),不带前面的命名空间名
     * @param id        id
     */
    public static void delRedisDimInfo(String tableName, String id) {
        Jedis jedis = RedisUtil.getJedis();
        jedis.auth(REDIS_PASSWORD);
        String redisKey = tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    //测试方法
    public static void show(String tableName, Tuple2<String, String>... columnValues) throws Exception {
        if (columnValues.length <= 0) {
            throw new RuntimeException(">>>至少查询一个维度条件!");
        }

        //Attention 拼接where条件
        StringBuilder whereSql = new StringBuilder(" where ");
        for (int i = 0; i < columnValues.length; i++) {
            //获取单个查询条件
            Tuple2<String, String> columnValue = columnValues[i];
            //列名
            String column = columnValue.f0;
            //列值
            String value = columnValue.f1;
            whereSql.append(column).append("='").append(value).append("'");
            //如果不是最后一个条件,那就叫and连接多个条件
            if (i < columnValues.length - 1) {
                whereSql.append(" and ");
            }
        }
        String querySql = "select * from " + tableName + whereSql.toString();
        System.out.println(">>>查询维度表sql语句:" + querySql);
        //List<JSONObject> jsonObjects = MyJdbcUtil.queryList(querySql, JSONObject.class, false);
        List<JSONObject> jsonObjects = PhoenixUtil.queryList(querySql, JSONObject.class, false);

        /*
         * Explain
         *  jsonObjects:[{"ID":"1001","NAME":"fucker","AGE":"9"},{...},...]
         *  jsonObjects.get(0):{"ID":"1001","NAME":"fucker","AGE":"9"}
         * */
        System.out.println(jsonObjects);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(getDimInfo("DIM_USER_INFO", "20"));
        if (conn != null) {
            //因为若redis中有缓存的话,就不会去phoenix连接了,所以conn为空
            conn.close();//关闭连接
        }
        jedisPool.close();

    }
}

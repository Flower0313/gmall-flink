package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.common.GmallConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Properties;

/**
 * @ClassName gmall-flink-PhoenixUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:21 - 周六
 * @Describe
 */
public class PhoenixUtil {
    private static Connection conn;

    //初始化连接方法
    private static Connection init() {
        try {
            Class.forName(GmallConfig.PHOENIX_DRIVER);
            Properties properties = new Properties();
            properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
            Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER, properties);
            //设置连接到的Phoenix的库
            connection.setSchema(GmallConfig.HBASE_SCHEMA);
            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }


    /**
     *
     * @param sql sql语句
     * @param cls
     * @param <T> 声明泛型方法
     * @return List<T>
     */
    public static <T> List<T> queryList(String sql, Class<T> cls) {
        return null;
    }
}

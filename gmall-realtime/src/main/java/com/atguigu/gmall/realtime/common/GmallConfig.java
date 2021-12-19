package com.atguigu.gmall.realtime.common;

/**
 * @ClassName gmall-flink-GmallConfig
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日18:53 - 周二
 * @Describe
 */
public class GmallConfig {
    //Phoenix库名
    public static final String HBASE_SCHEMA = "GMALL0726_REALTIME";

    //Phoenix驱动
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    //Phoenix连接参数
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop102,hadoop103,hadoop104:2181";

}

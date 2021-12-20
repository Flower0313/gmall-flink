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

    //mysql连接参数
    public static final String MYSQL_SERVER="jdbc:mysql://hadoop102:3306/gmall_realtime?characterEncoding=utf-8&useSSL=false";

    public static final String MYSQL_DRIVER="com.mysql.jdbc.Driver";

    public static final String MYSQL_TABLE="table_process";

}

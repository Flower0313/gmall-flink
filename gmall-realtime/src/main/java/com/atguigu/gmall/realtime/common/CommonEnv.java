package com.atguigu.gmall.realtime.common;

/**
 * @ClassName gmall-flink-CommonEnv
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月12日18:41 - 周日
 * @Describe
 */
public class CommonEnv {
    //集群主机
    public static final String HOSTNAME = "hadoop102";

    //数据库密码
    public static final String MYSQL_PASSWORD = "root";

    //数据库名称
    public static final String DATABASE = "gmall_flink";
    public static final String REAL_DATABASE = "gmall_realtime";


    //需要监控的表
    public static final String TABLE = "gmall_flink.base_trademark";

    //ods层行为数据
    public static final String ODS_DB_TOPIC = "ods_base_db";

    //
    public static final String DB_GROUP_ID = "ods_db_group";

    //ods层业务数据
    public static final String ODS_LOG_TOPIC = "ods_base_log";

    //
    public static final String LOG_GROUP_ID = "ods_dwd_base_log_app";
}

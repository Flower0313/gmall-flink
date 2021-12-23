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

    public static final String REDIS_PASSWORD = "w654646";


    //页面日志主题
    public static final String PAGE_LOG_TOPIC = "dwd_page_log";
    //用户跳出数主题
    public static final String USER_JUMP_TOPIC = "dwm_user_jump_detail";
    //启动日志主题
    public static final String START_LOG_TOPIC = "dwd_start_log";
    //曝光日志主题
    public static final String DISPLAY_LOG_TOPIC = "dwd_display_log";


    //订单主题
    public static final String ORDER_INFO_TOPIC = "dwd_order_info";
    //订单详细主题
    public static final String ORDER_DETAIL_TOPIC = "dwd_order_detail";
    //订单宽表主题
    public static final String ORDER_WIDE_TOPIC = "dwm_order_wide";
    //支付信息主题
    public static final String PAYMENT_INFO_TOPIC = "dwd_payment_info";
    //支付宽表
    public static final String PAYMENT_WIDE_TOPIC = "dwm_payment_wide";
    //首日访问主题
    public static final String UNIQUE_VISIT_TOPIC = "dwm_unique_visit";
    //购物车主题
    public static final String CART_INTO_TOPIC = "dwd_cart_info";
    //收藏主题
    public static final String FAVOR_INFO_TOPIC = "dwd_favor_info";
    //退款主题
    public static final String ORDER_REFUND_TOPIC = "dwd_order_refund_info";
    //评论主题
    public static final String COMMENT_INFO_TOPIC = "dwd_comment_info";

}

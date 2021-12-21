package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-OrderInfo
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日19:21 - 周五
 * @Describe DWM层订单表
 */

@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time; //yyyy-MM-dd HH:mm:ss
    String operate_time;
    String create_date; // 把其他字段处理得到
    String create_hour;
    Long create_ts;

}


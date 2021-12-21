package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-OrderDetail
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日19:24 - 周五
 * @Describe DWM层订单明细表
 */

@Data
public class OrderDetail {
    Long id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    String create_time;
    BigDecimal split_total_amount;
    BigDecimal split_activity_amount = BigDecimal.valueOf(0.0);
    BigDecimal split_coupon_amount = BigDecimal.valueOf(0.0);
    Long create_ts;
}

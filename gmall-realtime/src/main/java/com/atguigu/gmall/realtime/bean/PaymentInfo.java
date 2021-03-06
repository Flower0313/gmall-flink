package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-PaymentWide
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日9:28 - 周一
 * @Describe DWM层支付实体类
 */

@Data
public class PaymentInfo {
    Long id;
    Long order_id; //订单id
    Long user_id;
    BigDecimal total_amount; //订单中每个商品的金额
    String subject; //商品描述
    String payment_type; //支付类型
    String create_time;
    String callback_time;

}

package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.beans.BeanUtils;


import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-PaymentWide
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日10:54 - 周一
 * @Describe DWM层支付宽表类
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentWide {

    Long payment_id;
    String subject;
    String payment_type;
    String payment_create_time;
    String callback_time;
    Long detail_id;
    Long order_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    BigDecimal split_feight_fee;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;
    String order_create_time;

    //from Hbase唯独表
    String province_name;   //查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;

    Integer user_age;       //用户信息
    String user_gender;

    Long spu_id;           //作为维度数据 要关联进来
    Long tm_id;
    Long category3_id;
    String spu_name;
    String tm_name;
    String category3_name;

    public PaymentWide(PaymentInfo paymentInfo, OrderWide orderWide) {
        mergePaymentInfo(paymentInfo);
        mergeOrderWide(orderWide);
    }

    public void mergePaymentInfo(PaymentInfo paymentInfo) {
        if (paymentInfo != null) {
            /*
             * Attention 对于属性名称相同的所有情况，将属性值从源bean复制到目标bean
             *  这里用的BeanUtils是用的springframework的而不是common的,因为后者失效
             * */
            BeanUtils.copyProperties(paymentInfo, this);
            payment_create_time = paymentInfo.create_time;
            payment_id = paymentInfo.id;

        }
    }

    public void mergeOrderWide(OrderWide orderWide) {
        if (orderWide != null) {
            BeanUtils.copyProperties(orderWide, this);
            order_create_time = orderWide.create_time;
        }
    }

}

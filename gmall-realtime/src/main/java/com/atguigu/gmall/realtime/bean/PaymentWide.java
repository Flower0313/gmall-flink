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
    //from paymentInfo表
    String callback_time;
    String payment_type;
    Long user_id;
    String subject; //交易内容
    Long payment_id;
    String payment_create_time;
    String order_create_time;

    //from 公共字段
    BigDecimal total_amount;//支付金额
    Long order_id;


    //from OrderWide表
    Long category3_id;
    String category3_name;
    Long spu_id;//作为维度数据 要关联进来
    Long tm_id;
    String spu_name;//商品名称
    String tm_name;//品牌名称
    Long province_id;
    String province_name;//查询维表得到
    String province_area_code;
    String province_iso_code;
    String province_3166_2_code;
    Integer user_age;//用户信息
    String user_gender;
    Long detail_id;
    Long sku_id;
    Long sku_num;
    String sku_name; //sku名称
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;
    BigDecimal activity_reduce_amount; //活动折扣金额
    BigDecimal coupon_reduce_amount; // 优惠劵活动金额
    BigDecimal original_total_amount; //原价金额
    String order_status;
    BigDecimal feight_fee; //运费
    BigDecimal order_price;//购买价格(下单时sku价格)


    //from 未知
    BigDecimal split_feight_fee;


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

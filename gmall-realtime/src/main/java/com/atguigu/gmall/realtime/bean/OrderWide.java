package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-OrderWide
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日20:50 - 周五
 * @Describe DWM层订单宽表=订单表+订单明细表+所需的维度表
 */
@Data
@AllArgsConstructor
public class OrderWide {
    //from OrderDetail
    Long detail_id;
    Long sku_id;
    BigDecimal order_price;
    Long sku_num;
    String sku_name;
    BigDecimal split_activity_amount;
    BigDecimal split_coupon_amount;
    BigDecimal split_total_amount;

    //from OrderInfo
    Long province_id;
    String order_status;
    Long user_id;
    Long order_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String create_date;
    String create_hour;
    String create_time;

    //from Others(来自Hbase维度表)
    BigDecimal split_feight_fee;
    String expire_time;
    String operate_time;
    String province_name;//dim_base_province
    String province_area_code;//dim_base_province
    String province_iso_code;//dim_base_province
    String province_3166_2_code;//dim_base_province
    Integer user_age;//dim_user_info
    String user_gender;//dim_user_info
    Long spu_id;//dim_sku_info
    String spu_name;//dim_sku_info
    Long tm_id;//dim_base_trademark
    String tm_name;//dim_base_trademark
    Long category3_id;//dim_base_category3
    String category3_name;//dim_base_category3

    public OrderWide(OrderInfo orderInfo, OrderDetail orderDetail) {
        mergeOrderInfo(orderInfo);
        mergeOrderDetail(orderDetail);
    }

    public void mergeOrderInfo(OrderInfo orderInfo) {
        if (orderInfo != null) {
            this.order_id = orderInfo.id;
            this.order_status = orderInfo.order_status;
            this.create_time = orderInfo.create_time;
            this.create_date = orderInfo.create_date;
            this.create_hour = orderInfo.create_hour;
            this.activity_reduce_amount = orderInfo.activity_reduce_amount;
            this.coupon_reduce_amount = orderInfo.coupon_reduce_amount;
            this.original_total_amount = orderInfo.original_total_amount;
            this.feight_fee = orderInfo.feight_fee;
            this.total_amount = orderInfo.total_amount;
            this.province_id = orderInfo.province_id;
            this.user_id = orderInfo.user_id;
        }
    }

    public void mergeOrderDetail(OrderDetail orderDetail) {
        if (orderDetail != null) {
            this.detail_id = orderDetail.id;
            this.sku_id = orderDetail.sku_id;
            this.sku_name = orderDetail.sku_name;
            this.order_price = orderDetail.order_price;
            this.sku_num = orderDetail.sku_num;
            this.split_activity_amount = orderDetail.split_activity_amount;
            this.split_coupon_amount = orderDetail.split_coupon_amount;
            this.split_total_amount = orderDetail.split_total_amount;
        }
    }

    public void mergeOtherOrderWide(OrderWide otherOrderWide) {
        this.order_status = ObjectUtils.firstNonNull(this.order_status, otherOrderWide.order_status);
        this.create_time = ObjectUtils.firstNonNull(this.create_time, otherOrderWide.create_time);
        this.create_date = ObjectUtils.firstNonNull(this.create_date, otherOrderWide.create_date);
        this.coupon_reduce_amount = ObjectUtils.firstNonNull(this.coupon_reduce_amount, otherOrderWide.coupon_reduce_amount);
        this.activity_reduce_amount = ObjectUtils.firstNonNull(this.activity_reduce_amount, otherOrderWide.activity_reduce_amount);
        this.original_total_amount = ObjectUtils.firstNonNull(this.original_total_amount, otherOrderWide.original_total_amount);
        this.feight_fee = ObjectUtils.firstNonNull(this.feight_fee, otherOrderWide.feight_fee);
        this.total_amount = ObjectUtils.firstNonNull(this.total_amount, otherOrderWide.total_amount);
        this.user_id = ObjectUtils.<Long>firstNonNull(this.user_id, otherOrderWide.user_id);
        this.sku_id = ObjectUtils.firstNonNull(this.sku_id, otherOrderWide.sku_id);
        this.sku_name = ObjectUtils.firstNonNull(this.sku_name, otherOrderWide.sku_name);
        this.order_price = ObjectUtils.firstNonNull(this.order_price, otherOrderWide.order_price);
        this.sku_num = ObjectUtils.firstNonNull(this.sku_num, otherOrderWide.sku_num);
        this.split_activity_amount = ObjectUtils.firstNonNull(this.split_activity_amount);
        this.split_coupon_amount = ObjectUtils.firstNonNull(this.split_coupon_amount);
        this.split_total_amount = ObjectUtils.firstNonNull(this.split_total_amount);
    }


}

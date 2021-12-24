package com.atguigu.gmallpublishertest.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-ProductStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日16:08 - 周五
 * @Describe 商品交易统计实体类
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductStats {
    String stt;
    String edt;
    Long sku_id;
    String sku_name;
    BigDecimal sku_price;
    Long spu_id;
    String spu_name;
    Long tm_id;
    String tm_name;
    Long category3_id;
    String category3_name;
    @Builder.Default
    Long display_ct = 0L;
    @Builder.Default
    Long click_ct = 0L;
    @Builder.Default
    Long cart_ct = 0L;
    @Builder.Default
    Long order_sku_num = 0L;
    @Builder.Default
    Double order_amount = 0D;
    @Builder.Default
    Long order_ct = 0L;
    @Builder.Default
    Double payment_amount = 0D;
    @Builder.Default
    Long refund_ct = 0L;
    @Builder.Default
    Double refund_amount = 0D;
    @Builder.Default
    Long comment_ct = 0L;
    @Builder.Default
    Long good_comment_ct = 0L;
    Long ts;
}

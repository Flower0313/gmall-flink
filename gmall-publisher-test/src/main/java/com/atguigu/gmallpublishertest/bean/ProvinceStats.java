package com.atguigu.gmallpublishertest.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-ProvinceStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日19:37 - 周五
 * @Describe 地区交易额统计实体类
 */


@Builder
@AllArgsConstructor
@Data
@NoArgsConstructor
public class ProvinceStats {
    private String stt;
    private String edt;
    private String province_id;
    private String province_name;
    @Builder.Default
    private Double order_amount = 0D;
    private String ts;
}

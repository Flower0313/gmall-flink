package com.atguigu.gmallpublishertest.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.RoundingMode;
import java.text.DecimalFormat;

/**
 * @ClassName gmall-flink-VistorStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:20 - 周五
 * @Describe 流量访问统计实体类
 */

@Builder
@AllArgsConstructor
@Data
@NoArgsConstructor
public class VisitorStats {
    private String stt;
    private String edt;
    private String vc;
    private String ch;
    private String ar;
    private String is_new;
    @Builder.Default
    private Long uv_ct = 0L;//独立访客数,每日用户的第一次访问
    @Builder.Default
    private Long pv_ct = 0L;//页面访问数
    @Builder.Default
    private Long sv_ct = 0L;//进入次数
    @Builder.Default
    private Long uj_ct = 0L;//跳出次数
    @Builder.Default
    private Long dur_sum = 0L;//持续访问时间
    @Builder.Default
    private Long new_uv = 0L;
    private Long ts;
    private int hr;

    //计算跳出率
    public Double getUjRate() {
        if (uv_ct != 0L) {
            //若uv不是0,那么sv肯定就不是0
            double d = (double) uj_ct * 100 / sv_ct;
            return Double.valueOf(new DecimalFormat(".00").format(d));
        } else {
            return 0D;
        }
    }

    //计算每次访问停留时间(秒)  = 当日总停留时间（毫秒)/当日访问次数/1000
    public Double getDurPerSv() {
        if (uv_ct != 0L) {
            long l = dur_sum / sv_ct / 1000;
            return (double) l;
        } else {
            return 0D;
        }
    }

    //计算每次访问停留页面数 = 当日总访问页面数/当日访问次数
    public Double getPvPerSv() {
        if (uv_ct != 0L) {
            double d = (double) pv_ct / sv_ct;
            return Double.valueOf(new DecimalFormat(".00").format(d));
        } else {
            return 0D;
        }
    }


}

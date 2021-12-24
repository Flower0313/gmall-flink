package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @ClassName gmall-flink-VisitorStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日20:43 - 周一
 * @Describe DWS层访问主题宽表
 */

@Data
@AllArgsConstructor
public class VisitorStats {
    //统计开始时间--与clickHouse相关
    private String stt;
    //统计结束时间--与clickHouse相关
    private String edt;
    //维度：版本
    private String vc;
    //维度：渠道,来自
    private String ch;
    //维度：地区
    private String ar;
    //维度：新老用户标识
    private String is_new;
    //度量值：独立访客数
    private Long uv_ct = 0L;
    //度量值：页面浏览数
    private Long pv_ct = 0L;
    //度量值： 进入次数
    private Long sv_ct = 0L;
    //度量值： 跳出次数
    private Long uj_ct = 0L;
    //度量值： 持续访问时间,仅来自page_log
    private Long dur_sum = 0L;
    //统计时间
    private Long ts;
}

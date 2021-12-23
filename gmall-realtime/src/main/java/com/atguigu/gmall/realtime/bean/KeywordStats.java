package com.atguigu.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName gmall-flink-KeywordStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月23日22:09 - 周四
 * @Describe 关键词统计实体类
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String word;
    private Long ct;
    private String source;
    private String stt;
    private String edt;
    private Long ts;
}

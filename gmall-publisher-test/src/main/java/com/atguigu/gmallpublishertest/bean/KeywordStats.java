package com.atguigu.gmallpublishertest.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName gmall-flink-KeywordStats
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月25日1:15 - 周六
 * @Describe 词云实体类
 */

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class KeywordStats {
    private String stt;
    private String edt;
    private String keyword;
    @Builder.Default
    private Long ct = 0L;
    private String ts;
}

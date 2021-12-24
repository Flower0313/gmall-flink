package com.atguigu.gmallpublishertest.util;

import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.Date;

/**
 * @ClassName gmall-flink-GmallTime
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日21:04 - 周五
 * @Describe
 */
public class GmallTime {
    public static Integer getNowDate() {
        return Integer.valueOf(DateFormatUtils.format(new Date(), "yyyyMMdd"));
    }
}

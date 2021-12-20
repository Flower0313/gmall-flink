package com.atguigu.gmall.realtime.utils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @ClassName gmall-flink-DateTimeUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日14:16 - 周一
 * @Describe 时间工具类,为什么保证线程安全,不会让多线程的数据时间混杂在一起
 */
public class DateTimeUtil {
    //自定义时间格式
    private final static DateTimeFormatter formatter
            = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static String toYMDhms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return formatter.format(localDateTime);

    }

    public static Long toTs(String YmDHms) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, formatter);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }

}


package com.atguigu.gmallpublishertest.mapper;

import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-ProvinceStatsMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日19:42 - 周五
 * @Describe 地区统计接口
 */
public interface ProvinceStatsMapper {

    @Select("select" +
            "    province_name," +
            "sum(order_amount) order_amount " +
            "from province_stats where toYYYYMMDD(stt)=#{date} " +
            "group by province_name")
    public List<Map> selectProvinceStats(int date);
}

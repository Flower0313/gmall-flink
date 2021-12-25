package com.atguigu.gmallpublishertest.mapper;

import com.atguigu.gmallpublishertest.bean.KeywordStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-KeywordStatsMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月25日1:17 - 周六
 * @Describe
 */


public interface KeywordStatsMapper {
    @Select("select word," +
            "count(*) ct from keyword_stats " +
            "where toYYYYMMDD(stt)=#{date} group by word " +
            "order by ct desc limit #{limit}")
    public List<Map> selectKeyWordStats(@Param("date") int date, @Param("limit") int limit);
}

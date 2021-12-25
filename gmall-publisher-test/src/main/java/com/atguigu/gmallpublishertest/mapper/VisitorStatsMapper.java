package com.atguigu.gmallpublishertest.mapper;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-VisitorStatsMapper
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:40 - 周五
 * @Describe 访客流量统计
 */

public interface VisitorStatsMapper {
    @Select("select is_new,sum(uv_ct) uv_ct," +
            "       sum(pv_ct) pv_ct," +
            "       sum(sv_ct) sv_ct," +
            "       sum(uj_ct) uj_ct," +
            "       sum(dur_sum) dur_sum " +
            "from visitor_stats where toYYYYMMDD(stt)=#{date} " +
            "group by is_new")
    public List<Map> selectVisitorStatsByNewFlag(int date);

    @Select("select" +
            "    toHour(stt) hr," +
            "    sum(case when is_new='1' then 1 else 0 end) is_new," +
            "    sum(uv_ct) uv_ct," +
            "    sum(pv_ct) pv_ct " +
            "from visitor_stats where toYYYYMMDD(stt)=#{date} " +
            "group by toHour(stt)")
    public List<Map> selectVisitorStatsByHour(int date);
}

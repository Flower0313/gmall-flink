package com.atguigu.gmallpublishertest.service.impl;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import com.atguigu.gmallpublishertest.mapper.VisitorStatsMapper;
import com.atguigu.gmallpublishertest.service.VisitorStatService;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-VisitorStatServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:54 - 周五
 * @Describe
 */
public class VisitorStatServiceImpl implements VisitorStatService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public List<VisitorStats> getVisitorStatsByNewsFlag(int date) {
        List<Map> stats = visitorStatsMapper.selectVisitorStatsByNewFlag(date);

        ArrayList<VisitorStats> visitorStats = new ArrayList<>();

        for (Map stat : stats) {
            VisitorStats build = VisitorStats.builder()
                    .is_new(String.valueOf(stat.get("is_new")))
                    .uv_ct(Long.valueOf(String.valueOf(stat.get("uv_ct"))))
                    .build();
            visitorStats.add(build);
        }
        return visitorStats;
    }
}

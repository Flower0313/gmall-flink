package com.atguigu.gmallpublishertest.service.impl;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import com.atguigu.gmallpublishertest.mapper.VisitorStatsMapper;
import com.atguigu.gmallpublishertest.service.VisitorStatService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-VisitorStatServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:54 - 周五
 * @Describe
 */

@Service
public class VisitorStatServiceImpl implements VisitorStatService {
    @Autowired
    VisitorStatsMapper visitorStatsMapper;

    @Override
    public Map<String, VisitorStats> getVisitorStatsByNewsFlag(int date) {
        List<Map> stats = visitorStatsMapper.selectVisitorStatsByNewFlag(date);

        HashMap<String, VisitorStats> statsHashMap = new HashMap<>();

        for (Map stat : stats) {
            String is_new = String.valueOf(stat.get("is_new"));
            String user = ("1").equals(is_new) ? "新用户" : "老用户";
            VisitorStats build = VisitorStats.builder()
                    .is_new(user)
                    .uv_ct(Long.valueOf(String.valueOf(stat.get("uv_ct"))))
                    .pv_ct(Long.valueOf(String.valueOf(stat.get("pv_ct"))))
                    .sv_ct(Long.valueOf(String.valueOf(stat.get("sv_ct"))))
                    .uj_ct(Long.valueOf(String.valueOf(stat.get("uj_ct"))))
                    .dur_sum(Long.valueOf(String.valueOf(stat.get("dur_sum"))))
                    .build();
            if ("1".equals(is_new)) {
                statsHashMap.put("new", build);
            } else {
                statsHashMap.put("old", build);
            }
        }
        return statsHashMap;
    }
}

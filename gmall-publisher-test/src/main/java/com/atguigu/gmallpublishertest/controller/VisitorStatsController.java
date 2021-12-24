package com.atguigu.gmallpublishertest.controller;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import com.atguigu.gmallpublishertest.mapper.VisitorStatsMapper;
import com.atguigu.gmallpublishertest.service.VisitorStatService;
import com.atguigu.gmallpublishertest.util.GmallTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @ClassName gmall-flink-VisitorStatsController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:59 - 周五
 * @Describe
 */
@RestController
public class VisitorStatsController {
    @Autowired
    VisitorStatService visitorStatService;

    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = GmallTime.getNowDate();
        }

        List<VisitorStats> stats = visitorStatService.getVisitorStatsByNewsFlag(date);


        return "";
    }
}

package com.atguigu.gmallpublishertest.service;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-VisitorStatService
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:52 - 周五
 * @Describe
 */

@Service
public interface VisitorStatService {
    //新老访客流量统计
    public Map<String,VisitorStats> getVisitorStatsByNewsFlag(int date);
}

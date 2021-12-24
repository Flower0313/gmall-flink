package com.atguigu.gmallpublishertest.service;

import com.atguigu.gmallpublishertest.bean.ProvinceStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName gmall-flink-ProvinceStatsService
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日19:40 - 周五
 * @Describe 省份统计接口
 */

@Service
public interface ProvinceStatsService {
    //统计地区维度
    public List<ProvinceStats> getProvinceStats(int date);
}

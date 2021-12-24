package com.atguigu.gmallpublishertest.service.impl;

import com.atguigu.gmallpublishertest.bean.ProductStats;
import com.atguigu.gmallpublishertest.bean.ProvinceStats;
import com.atguigu.gmallpublishertest.mapper.ProductStatsMapper;
import com.atguigu.gmallpublishertest.mapper.ProvinceStatsMapper;
import com.atguigu.gmallpublishertest.service.ProvinceStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-ProvinceStatsServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日19:40 - 周五
 * @Describe 省份接口实现类
 */

@Service
public class ProvinceStatsServiceImpl implements ProvinceStatsService {
    @Autowired
    ProvinceStatsMapper provinceStatsMapper;

    @Override
    public List<ProvinceStats> getProvinceStats(int date) {
        List<Map> provinces = provinceStatsMapper.selectProvinceStats(date);
        ArrayList<ProvinceStats> provinceStats = new ArrayList<>();


        for (Map province : provinces) {
            ProvinceStats build = ProvinceStats.builder()
                    .province_name(String.valueOf(province.get("province_name")))
                    .order_amount(Double.valueOf(String.valueOf(province.get("order_amount"))))
                    .build();
            provinceStats.add(build);
        }

        return provinceStats;
    }
}

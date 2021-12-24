package com.atguigu.gmallpublishertest.service.impl;

import com.atguigu.gmallpublishertest.bean.ProductStats;
import com.atguigu.gmallpublishertest.mapper.ProductStatsMapper;
import com.atguigu.gmallpublishertest.service.ProductStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.atguigu.gmallpublishertest.bean.ProductStats;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-SugarServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日1:37 - 周五
 * @Describe Sugar的实现类
 */

@Service
public class ProductStatsServiceImpl implements ProductStatsService {
    @Autowired
    ProductStatsMapper productStatsMapper;

    @Override
    public BigDecimal getGMV(int date) {
        //这两个getGMV不是同一个,只是名字相同,return中调用的是接口中的getGMV
        return productStatsMapper.getGMV(date);
    }

    @Override
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit) {
        List<Map> stats = productStatsMapper.getProductStatsGroupByCategory3(date, limit);

        ArrayList<ProductStats> productStats = new ArrayList<>();
        for (Map stat : stats) {
            ProductStats build = ProductStats.builder()
                    .category3_name((String) stat.get("category3_name"))
                    .order_amount(Double.valueOf(String.valueOf(stat.get("order_amount"))))
                    .build();
            productStats.add(build);
        }
        return productStats;
    }


    /**
     * 将List(Map["tm_id":"9","tm_name":"华为"],Map["tm_id":"10","tm_name":"apple"],...)
     * ==> List(ProductStats(..),ProductStats(..),...)
     * <p>
     * 这里需要的字段是tm_name,和聚合的order_amount
     *
     * @param date  日期
     * @param limit 数量
     * @return 相当于扁平化, 将收到的List<Map> ==> List<ProductStats>
     */
    @Override
    public List<ProductStats> getProductStatsByTrademark(int date, int limit) {
        List<Map> stats = productStatsMapper.getProductStatsByTrademark(date, limit);

        ArrayList<ProductStats> productStats = new ArrayList<>();
        for (Map stat : stats) {
            ProductStats build = ProductStats.builder()
                    .tm_name((String) stat.get("tm_name"))
                    .order_amount(Double.valueOf(String.valueOf(stat.get("order_amount"))))
                    .build();
            productStats.add(build);
        }
        return productStats;
    }


    @Override
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit) {
        List<Map> stats = productStatsMapper.getProductStatsGroupBySpu(date, limit);

        ArrayList<ProductStats> productStats = new ArrayList<>();
        for (Map stat : stats) {
            ProductStats build = ProductStats.builder()
                    .spu_id(Long.valueOf(String.valueOf(stat.get("spu_id"))))
                    .spu_name((String) stat.get("spu_name"))
                    .order_amount(Double.valueOf(String.valueOf(stat.get("order_amount"))))
                    .order_ct(Long.valueOf(String.valueOf(stat.get("order_ct"))))
                    .build();
            productStats.add(build);
        }
        return productStats;
    }


}

package com.atguigu.gmallpublishertest.service;

import com.atguigu.gmallpublishertest.bean.ProductStats;
import com.atguigu.gmallpublishertest.bean.ProvinceStats;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-SugarService
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日1:36 - 周五
 * @Describe 接口
 */

@Service
public interface ProductStatsService {
    //获取某一天的总交易额
    public BigDecimal getGMV(int date);

    //统计某天不同类别商品交易额排名
    public List<ProductStats> getProductStatsGroupByCategory3(int date, int limit);

    //统计某天不同品牌商品交易额排名
    public List<ProductStats> getProductStatsByTrademark(int date, int limit);

    //统计某天不同SPU商品交易额排名
    public List<ProductStats> getProductStatsGroupBySpu(int date, int limit);
}

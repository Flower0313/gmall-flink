package com.atguigu.gmallpublishertest.service;

import com.atguigu.gmallpublishertest.bean.KeywordStats;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @ClassName gmall-flink-KeywordStatsService
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月25日1:29 - 周六
 * @Describe
 */
@Service
public interface KeywordStatsService {
    public List<KeywordStats> getKeywordStats(int date, int limit);
}

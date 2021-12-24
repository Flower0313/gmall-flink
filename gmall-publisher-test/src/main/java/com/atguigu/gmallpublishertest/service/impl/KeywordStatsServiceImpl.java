package com.atguigu.gmallpublishertest.service.impl;

import com.atguigu.gmallpublishertest.bean.KeywordStats;
import com.atguigu.gmallpublishertest.mapper.KeywordStatsMapper;
import com.atguigu.gmallpublishertest.service.KeywordStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-KeywordStatsServiceImpl
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月25日1:30 - 周六
 * @Describe
 */
@Service
public class KeywordStatsServiceImpl implements KeywordStatsService {
    @Autowired
    KeywordStatsMapper keywordStatsMapper;

    @Override
    public List<KeywordStats> getKeywordStats(int date, int limit) {
        List<Map> stats = keywordStatsMapper.selectKeyWordStats(date, limit);

        ArrayList<KeywordStats> keywordStats = new ArrayList<>();

        for (Map stat : stats) {
            KeywordStats build = KeywordStats.builder()
                    .ct(Long.valueOf(String.valueOf(stat.get("ct"))))
                    .keyword(String.valueOf(stat.get("word")))
                    .build();
            keywordStats.add(build);
        }
        return keywordStats;
    }
}

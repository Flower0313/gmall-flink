package com.atguigu.gmallpublishertest.controller;

import com.atguigu.gmallpublishertest.bean.KeywordStats;
import com.atguigu.gmallpublishertest.service.KeywordStatsService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName gmall-flink-KeywordStatsController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月25日1:35 - 周六
 * @Describe
 */

@RestController
public class KeywordStatsController {
    @Autowired
    KeywordStatsService keywordStatsService;


    @RequestMapping("/keyword")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date,
                                   @RequestParam(value = "limit", defaultValue = "20") int limit
    ) {
        List<KeywordStats> keywordStats = keywordStatsService.getKeywordStats(date, limit);
        StringBuilder jsons = new StringBuilder("{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\":");
        ArrayList<String> middleJson = new ArrayList<>();
        for (KeywordStats keywordStat : keywordStats) {
            String json = "{\"name\":\"" + keywordStat.getKeyword() + "\"," +
                    "\"value\":\"" + keywordStat.getCt() + "\"}";
            middleJson.add(json);
        }
        jsons.append(StringUtils.join(middleJson)).append("}");
        return jsons.toString();
    }

}

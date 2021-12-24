package com.atguigu.gmallpublishertest.controller;

import com.atguigu.gmallpublishertest.bean.ProvinceStats;
import com.atguigu.gmallpublishertest.service.ProvinceStatsService;
import com.atguigu.gmallpublishertest.util.GmallTime;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @ClassName gmall-flink-ProvinceStatsController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日19:57 - 周五
 * @Describe
 */

@RestController
public class ProvinceStatsController {
    @Autowired
    ProvinceStatsService provinceStatsService;

    @RequestMapping("/province")
    public String getProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            //若请求中没有赋值导致date为默认值,就以当前日期为参数
            date = GmallTime.getNowDate();
        }

        List<ProvinceStats> provinceStats = provinceStatsService.getProvinceStats(date);
        ArrayList<String> jsons = new ArrayList<>();

        for (ProvinceStats provinceStat : provinceStats) {
            String json = "{\"name\":\"" + provinceStat.getProvince_name() + "\"," +
                    "\"value\":" + provinceStat.getOrder_amount() + "}";
            jsons.add(json);
        }
        StringBuilder resultJson = new StringBuilder("{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"mapData\":" + StringUtils.join(jsons)
                + "}}");

        return resultJson.toString();
    }

    public Integer getNowDate() {
        return Integer.valueOf(DateFormatUtils.format(new Date(), "yyyyMMdd"));
    }

}

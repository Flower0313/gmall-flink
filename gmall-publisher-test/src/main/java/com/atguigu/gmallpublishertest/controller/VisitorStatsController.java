package com.atguigu.gmallpublishertest.controller;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import com.atguigu.gmallpublishertest.mapper.VisitorStatsMapper;
import com.atguigu.gmallpublishertest.service.VisitorStatService;
import com.atguigu.gmallpublishertest.util.GmallTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-VisitorStatsController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日20:59 - 周五
 * @Describe
 */
@RestController
public class VisitorStatsController {
    @Autowired//自动寻找这个接口的实现类
    VisitorStatService visitorStatService;

    @RequestMapping("/newflag")
    public String getVisitorStatsByNewFlag(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = GmallTime.getNowDate();
        }
        Map<String, VisitorStats> stats = visitorStatService.getVisitorStatsByNewsFlag(date);

        VisitorStats aNew = stats.get("new");
        System.out.println("new>>>" + aNew);

        System.out.println("old>>>" + stats.get("old"));

        /*StringBuilder jsons = new StringBuilder("{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"total\": 2," +
                "    \"columns\": [" +
                "      {" +
                "        \"name\": \"用户\"," +
                "        \"id\": \"user\"" +
                "      }," +
                "      {" +
                "        \"name\": \"跳出率\"," +
                "        \"id\": \"UjRate\"" +
                "      }," +
                "      {" +
                "        \"name\": \"访问次数\"," +
                "        \"id\": \"PvPerSv\"" +
                "      }," +
                "      {" +
                "        \"name\": \"平均访问时长\"," +
                "        \"id\": \"DurPerSv\"" +
                "      }" +
                "    ]," +
                "    \"rows\":[ ");

        VisitorStats aNew = stats.get("new");
        jsons.append("{\"user\":\"" + aNew.getIs_new() + "\"," +
                "\"UjRate\":" + aNew.getUjRate() + "," +
                "\"PvPerSv\":" + aNew.getPvPerSv() + "," +
                "\"DurPerSv\":" + aNew.getDurPerSv() + "},");
        VisitorStats old = stats.get("old");
        jsons.append("{\"user\":\"" + old.getIs_new() + "\"," +
                "\"UjRate\":" + old.getUjRate() + "," +
                "\"PvPerSv\":" + old.getPvPerSv() + "," +
                "\"DurPerSv\":" + old.getDurPerSv() + "}");
        jsons.append("]}}");*/

        return "success";
    }
}

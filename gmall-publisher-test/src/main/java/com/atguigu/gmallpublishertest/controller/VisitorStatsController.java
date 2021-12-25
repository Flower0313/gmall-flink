package com.atguigu.gmallpublishertest.controller;

import com.atguigu.gmallpublishertest.bean.VisitorStats;
import com.atguigu.gmallpublishertest.mapper.VisitorStatsMapper;
import com.atguigu.gmallpublishertest.service.VisitorStatService;
import com.atguigu.gmallpublishertest.util.GmallTime;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
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

        StringBuilder jsons = new StringBuilder("{" +
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
        if (aNew != null) {
            jsons.append("{\"user\":\"" + aNew.getIs_new() + "\"," +
                    "\"UjRate\":" + aNew.getUjRate() + "," +
                    "\"PvPerSv\":" + aNew.getPvPerSv() + "," +
                    "\"DurPerSv\":" + aNew.getDurPerSv() + "},");
        } else {
            jsons.append("{\"user\":\"" + "新用户" + "\"," +
                    "\"UjRate\":" + 0.0 + "," +
                    "\"PvPerSv\":" + 0.0 + "," +
                    "\"DurPerSv\":" + 0.0 + "},");
        }

        VisitorStats old = stats.get("old");
        if (old != null) {
            jsons.append("{\"user\":\"" + old.getIs_new() + "\"," +
                    "\"UjRate\":" + old.getUjRate() + "," +
                    "\"PvPerSv\":" + old.getPvPerSv() + "," +
                    "\"DurPerSv\":" + old.getDurPerSv() + "}");
        } else {
            jsons.append("{\"user\":\"" + "；老用户" + "\"," +
                    "\"UjRate\":" + 0.0 + "," +
                    "\"PvPerSv\":" + 0.0 + "," +
                    "\"DurPerSv\":" + 0.0 + "}");
        }
        jsons.append("]}}");

        return jsons.toString();
    }

    @RequestMapping("/hr")
    public String getMidStatsGroupByHour(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = GmallTime.getNowDate();
        }

        List<VisitorStats> statsGroupByHour = visitorStatService.getMidStatsGroupByHour(date);

        //构建对应的24小时数组(0~23)
        VisitorStats[] visitorStatsArr = new VisitorStats[24];
        //在对应的小时索引放入位置
        for (VisitorStats visitorStats : statsGroupByHour) {
            visitorStatsArr[visitorStats.getHr()] = visitorStats;
        }
        ArrayList<Integer> hrs = new ArrayList<>();
        ArrayList<Long> isNews = new ArrayList<>();
        ArrayList<Long> uvs = new ArrayList<>();
        ArrayList<Long> pvs = new ArrayList<>();

        for (int i = 0; i <= 23; i++) {
            VisitorStats temp = null;
            if ((temp = visitorStatsArr[i]) == null) {
                hrs.add(i);
                isNews.add(0L);
                uvs.add(0L);
                pvs.add(0L);
            } else {
                hrs.add(temp.getHr());
                isNews.add(Long.valueOf(temp.getIs_new()));
                uvs.add(temp.getUv_ct());
                pvs.add(temp.getPv_ct());
            }
        }

        StringBuilder jsons = new StringBuilder("{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\":[\"");
        jsons.append(StringUtils.join(hrs, "\",\""));
        //添加新用户信息
        jsons.append("\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"新用户\"," +
                "        \"data\":");
        //循环新用户
        jsons.append(StringUtils.join(isNews));

        //添加uv信息
        jsons.append("}," +
                "      {" +
                "        \"name\": \"uv\"," +
                "        \"data\": ");
        jsons.append(StringUtils.join(uvs));

        //添加pv信息
        jsons.append("}," +
                "\t  {" +
                "        \"name\": \"pv\"," +
                "        \"data\": ");
        jsons.append(StringUtils.join(pvs));

        jsons.append("}" +
                "    ]" +
                "  }" +
                "}");

        return jsons.toString();
    }
}

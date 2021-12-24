package com.atguigu.gmallpublishertest.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmallpublishertest.bean.ProductStats;
import com.atguigu.gmallpublishertest.service.ProductStatsService;
import com.atguigu.gmallpublishertest.util.GmallTime;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;

/**
 * @ClassName gmall-flink-SugarController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日1:35 - 周五
 * @Describe
 */

@RestController
public class ProductStatsController {
    @Autowired//它会找到它的实现类,也就是ProductStatsServiceImpl
    ProductStatsService productStatsService;

    /**
     * 请求api格式:localhost:8070/api/sugar/gmv?date=日期
     * 返回的json数据格式,status={1:今天,0:昨天}
     * {
     * "status": 0,
     * "data": 1201081.1632389291
     * }
     *
     * @param date 日期参数,默认值为0
     * @return 统计数据
     * @defaultValue 是给date参数默认值
     */
    @RequestMapping("/gmv")
    public String getGMV(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        int status = 0;
        if (date == 0) {
            status = 1;
            //若请求中没有赋值导致date为默认值,就以当前日期为参数
            date = GmallTime.getNowDate();
        }

        HashMap<String, Object> result = new HashMap<>();
        result.put("status", status);//注意这里status必须为Int格式的,不然sugarBI解析不正确
        result.put("data", productStatsService.getGMV(date));

        /*
         * Explain
         *  由于有@Autowired的存在,这里可以直接调用getGMV,其实是调用它的实现类getGMV,
         *  然后就会调用到productStatsMapper的getGMV方法
         * */
        return JSON.toJSONString(result);
    }


    /**
     * 返回的数据格式:
     * {
     * "status": 0,
     * "data": [
     * {
     * "name": "数码类",
     * "value": 371570
     * },
     * {
     * "name": "日用品",
     * "value": 296016
     * }
     * ]
     * }
     *
     * @return 返回json格式
     * @param1 日期
     * @param2 显示条数
     */
    @RequestMapping("/category3")
    public String getProductStatsGroupByCategory3(
            @RequestParam(value = "date", defaultValue = "20201225") Integer date,
            @RequestParam(value = "limit", defaultValue = "4") Integer limit
    ) {

        if (date == 0) {
            date = GmallTime.getNowDate();
        }
        //取出从接口来的数据,将数据遍历循环添加到ArrayList中
        List<ProductStats> stats = productStatsService.getProductStatsGroupByCategory3(date, limit);
        ArrayList<String> category_names = new ArrayList<>();
        ArrayList<Double> order_amounts = new ArrayList<>();
        for (ProductStats stat : stats) {
            category_names.add(stat.getCategory3_name());
            order_amounts.add(stat.getOrder_amount());
        }
        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(category_names, "\",\"")
                + "\"],\"series\": [" +
                "      {" + "\"name\": \"品牌\"," +
                "        \"data\": [" +
                StringUtils.join(order_amounts, ",") +
                "]}]}}";
    }

    /*
     * 输出的数据格式:
     *┬─tm_name───┬─order_amount─┐
     *│ 苹果       │     16394.00 │
     *│ 小米       │      8697.00 │
     *│ 香奈儿     │       900.00 │
     *│ 索芙特     │       258.00 │
     *│ CAREMiLLE │        69.00 │
     *┴───────────┴──────────────┘
     * */
    @RequestMapping("/trademark")
    public String getProductStatsByTrademark(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "5") int limit) {
        if (date == 0) {
            date = GmallTime.getNowDate();
        }
        //取出从接口来的数据,将数据遍历循环添加到ArrayList中
        List<ProductStats> stats = productStatsService.getProductStatsByTrademark(date, limit);
        ArrayList<String> tm_names = new ArrayList<>();
        ArrayList<Double> order_amounts = new ArrayList<>();
        //循环取出每条数据
        for (ProductStats stat : stats) {
            tm_names.add(stat.getTm_name());
            order_amounts.add(stat.getOrder_amount());
        }
        return "{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"categories\": [\"" +
                StringUtils.join(tm_names, "\",\"")
                + "\"]," +
                "    \"series\": [" +
                "      {" +
                "        \"name\": \"品牌\"," +
                "        \"data\": [" +
                StringUtils.join(order_amounts, ",") +
                "        ]}]}}";
    }


    @RequestMapping("/spu")
    public String getProductStatsGroupBySpu(
            @RequestParam(value = "date", defaultValue = "0") Integer date,
            @RequestParam(value = "limit", defaultValue = "10") int limit) {
        if (date == 0) {
            date = GmallTime.getNowDate();
        }
        //取出从接口来的数据,将数据遍历循环添加到ArrayList中
        List<ProductStats> stats = productStatsService.getProductStatsGroupBySpu(date, limit);
        ArrayList<String> middleJson = new ArrayList<>();

        //循环拼接json中间数据格式
        for (ProductStats stat : stats) {
            String json = "{\"number\":\"" + stat.getSpu_id() + "\"," +
                    "\"name\":\"" + stat.getSpu_name() + "\"," +
                    "\"value\":\"" + stat.getOrder_amount() + "\"," +
                    "\"cnt\":\"" + stat.getOrder_ct() + "\"}";
            middleJson.add(json);
        }


        //拼接开头json格式
        StringBuilder json = new StringBuilder("{" +
                "  \"status\": 0," +
                "  \"msg\": \"\"," +
                "  \"data\": {" +
                "    \"columns\": [" +
                "      {" +
                "        \"name\": \"商品编号\"," +
                "        \"id\": \"number\"" +
                "      }," +
                "      {" +
                "        \"name\": \"商品名称\"," +
                "        \"id\": \"name\"" +
                "      }," +
                "      {" +
                "        \"name\": \"销售额\"," +
                "        \"id\": \"value\"" +
                "      }," +
                "      {" +
                "        \"name\": \"销售次数\"," +
                "        \"id\": \"cnt\"" +
                "      }" +
                "    ]," +
                "    \"rows\":");
        json.append(StringUtils.join(middleJson)).append("}}");

        return json.toString();

    }

}

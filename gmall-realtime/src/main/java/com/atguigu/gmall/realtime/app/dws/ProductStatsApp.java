package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.HashSet;

/**
 * @ClassName gmall-flink-ProductStatsApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月21日19:47 - 周二
 * @Describe 商品统计实体类
 * 1.JSONObject的数据时用{}来表示的:
 * {"id":"123","age":"12","name":"博客园","time":2020-06-06}
 * <p>
 * 2.JSONArray是用JSONObject构成的数组:
 * [{"id":"123","age":"12","name":"博客园"},{"id":"321","age":"12","name":"csdn"}]
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //消费者组
        String groupId = "product_stats_app";
        //数据来源
        String pageViewSourceTopic = "dwd_page_log";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String cartInfoSourceTopic = "dwd_cart_info";
        String favorInfoSourceTopic = "dwd_favor_info";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //页面日志
        DataStreamSource<String> pageViewDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\pageLog.txt");
        //收藏订单数据
        DataStreamSource<String> favorInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\favorinfo.txt");
        //下单流数据
        DataStreamSource<String> orderWideDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderwide.txt");

        DataStreamSource<String> paymentWideDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\paymentInfo.txt");

        DataStreamSource<String> cartInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\cartinfo.txt");

        DataStreamSource<String> refundInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\refundinfo.txt");

        DataStreamSource<String> commentInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\commentinfo.txt");


        //Step-2.1 转换曝光及页面流数据
        SingleOutputStreamOperator<ProductStats> pageAndDisplayStatsDS = pageViewDS.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, ProductStats>.Context ctx, Collector<ProductStats> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(value);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");

                        if (pageId == null) {
                            System.out.println("pageId为空:" + jsonObj);
                        }
                        Long ts = jsonObj.getLong("ts");
                        //Attention 跳转到商品详情页(good_detail)就是点击数据,此商品的点击次数+1
                        if ("good_detail".equals(pageId) && "sku_id".equals(pageJsonObj.getString("item_type"))) {
                            Long skuId = pageJsonObj.getLong("item");
                            ProductStats productStats = ProductStats.builder()
                                    .sku_id(skuId)
                                    .click_ct(1L)//点击的次数
                                    .ts(ts)
                                    .build();
                            out.collect(productStats);//这不是return,下面的代码还是可以执行的
                        }

                        //Attention 尝试取出曝光数据,若曝光数据中也有sku也取出,然后此商品的曝光次数+1
                        JSONArray displays = jsonObj.getJSONArray("displays");
                        if (displays != null && displays.size() > 0) {//若曝光日志不为空
                            for (int i = 0; i < displays.size(); i++) {
                                //取出单条display数据
                                JSONObject display = displays.getJSONObject(i);
                                if ("sku_id".equals(display.getString("item_type"))) {
                                    Long skuId = display.getLong("item");
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId)
                                            .display_ct(1L)//曝光的次数
                                            .ts(ts).build();
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );

        //Step-2.2 转换收藏流数据
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDS.map(
                json -> {
                    JSONObject favorInfo = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(favorInfo.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(favorInfo.getLong("sku_id"))
                            .favor_ct(1L)//收藏次数+1
                            .ts(ts)
                            .build();
                }
        );

        //Step-2.3 转换下单流数据
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                json -> {
                    //将json转为换OrderWide类
                    OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    System.out.println("orderWide:===" + orderWide);
                    String create_time = orderWide.getCreate_time();
                    Long ts = DateTimeUtil.toTs(create_time);
                    return ProductStats.builder()
                            .sku_id(orderWide.getSku_id())
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount())
                            .ts(ts)
                            .build();
                }
        );


        pageAndDisplayStatsDS.print(">>>");


        env.execute();

    }
}

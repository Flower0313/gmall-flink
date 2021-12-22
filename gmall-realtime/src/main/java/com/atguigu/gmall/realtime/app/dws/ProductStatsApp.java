package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.bean.ProductStats;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.ParseException;
import java.time.Duration;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

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
        env.setParallelism(1);

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
        //DataStreamSource<String> pageViewDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\pageLog.txt");
        DataStreamSource<String> pageViewDS = env.addSource(MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId));

        //收藏订单数据
        //DataStreamSource<String> favorInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\favorinfo.txt");
        DataStreamSource<String> favorInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId));

        //下单流
        //DataStreamSource<String> orderWideDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderwide.txt");
        DataStreamSource<String> orderWideDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //支付宽流
        //DataStreamSource<String> paymentWideDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\paymentwide.txt");
        DataStreamSource<String> paymentWideDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId));

        //购物车流
        //DataStreamSource<String> cartInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\cartinfo.txt");
        DataStreamSource<String> cartInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId));

        //退款流
        //DataStreamSource<String> refundInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\refundinfo.txt");
        DataStreamSource<String> refundInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId));

        //评论流
        //DataStreamSource<String> commentInfoDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\commentinfo.txt");
        DataStreamSource<String> commentInfoDS = env.addSource(MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId));

        //Step-2.1 转换曝光及页面流数据,取出sku商品曝光的次数
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

        //Step-2.3 转换购物车流数据
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDS.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .ts(ts)
                            .cart_ct(1L)
                            .build();
                }
        );


        //Step-2.4 转换下单流数据(有JavaBean)
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDS.map(
                json -> {
                    //将json转为换OrderWide类
                    OrderWide orderWide = JSON.parseObject(json, OrderWide.class);
                    //System.out.println("orderWide:===" + orderWide);
                    String create_time = orderWide.getCreate_time();
                    Long ts = DateTimeUtil.toTs(create_time);
                    return ProductStats.builder()
                            .sku_id(orderWide.getSku_id())
                            /*
                             * Explain
                             *  Collections.singleton返回的是Set集合,因为你直接new HashSet(order_id)是添加不进值的,
                             *  因为它没有这种构造方法
                             * */
                            .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                            .order_sku_num(orderWide.getSku_num())
                            .order_amount(orderWide.getSplit_total_amount())
                            .ts(ts)
                            .build();
                }
        );

        //Step-2.5 转换支付流数据(有JavaBean)
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDS.map(
                json -> {
                    PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                    Long ts = DateTimeUtil.toTs(paymentWide.getPayment_create_time());
                    return ProductStats.builder()
                            .sku_id(paymentWide.getSku_id())
                            .payment_amount(paymentWide.getSplit_total_amount())
                            .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                            .ts(ts)
                            .build();
                }
        );

        //Step-2.6 转换退款流数据
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDS.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    return ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .refund_amount(jsonObj.getBigDecimal("refund_amount"))
                            .refundOrderIdSet(new HashSet(Collections.singleton(jsonObj.getLong("order_id"))))
                            .ts(ts)
                            .build();
                }
        );

        //Step-2.7 转换评价流数据
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDS.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    Long ts = DateTimeUtil.toTs(jsonObj.getString("create_time"));
                    long goodCt = GmallConstant.APPRAISE_GOOD.equals(jsonObj.getString("appraise")) ? 1L : 0L;
                    return ProductStats.builder()
                            .sku_id(jsonObj.getLong("sku_id"))
                            .comment_ct(1L)//评价数
                            .good_comment_ct(goodCt)//好评数
                            .ts(ts).build();
                }
        );

        //Step-3 7流合一
        DataStream<ProductStats> productStatDetailDStream = pageAndDisplayStatsDS
                .union(orderWideStatsDS, cartStatsDS,
                        paymentStatsDS, refundStatsDS, favorStatsDS,
                        commonInfoStatsDS);

        //Step-4 设定事件时间与水位线
        SingleOutputStreamOperator<ProductStats> productStatsWithTs = productStatDetailDStream.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((productStats, record) -> {
                            return productStats.getTs();
                        }));

        //Step-5 分组(按sku_id)、开窗(10秒)、聚合,这一步保证了一个窗口中sku_id唯一
        SingleOutputStreamOperator<ProductStats> productStatsDS = productStatsWithTs.keyBy(ProductStats::getSku_id)
                /*
                 * Explain 注意
                 *  这里需要7个流的数据都走到同样的水位线才会触发窗口,也就是一个流即使走了很远,但只要一个流没有达到触发窗口
                 *  的地步,这个窗口内的数据就不会输出。这样就可以解释paymentWide数据晚来的情况怎么处理了,就算paymentWide晚
                 *  来了很久,但只要一来,还是输出的当时的事件时间,所以才能做到7流的时间同步
                 * */
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProductStats>() {
                            @Override
                            public ProductStats reduce(ProductStats stats1, ProductStats stats2) throws Exception {
                                //聚合统计值
                                stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                                stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                                stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                                stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                                stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                                //将hashset聚合插入,这种结构会自动去重
                                stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                                //stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                                stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                                stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                                stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                                //stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                                stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));

                                stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                                //stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);

                                stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                                stats1.setGood_comment_ct(stats1.getGood_comment_ct() + stats2.getGood_comment_ct());
                                return stats1;
                            }
                        },
                        new WindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                            @Override
                            public void apply(Long aLong, TimeWindow window, Iterable<ProductStats> input, Collector<ProductStats> out) throws Exception {
                                ProductStats productStats = input.iterator().next();
                                //当上面聚合完成后,这里去重后统计个数
                                productStats.setOrder_ct((long) productStats.getOrderIdSet().size());
                                productStats.setRefund_order_ct((long) productStats.getRefundOrderIdSet().size());
                                productStats.setPaid_order_ct((long) productStats.getPaidOrderIdSet().size());

                                //设置窗口的开始和结束时间
                                productStats.setStt(DateTimeUtil.toYMDhms(new Date(window.getStart())));
                                productStats.setEdt(DateTimeUtil.toYMDhms(new Date(window.getEnd())));
                                productStats.setTs(new Date().getTime());//获取当前时间戳

                                out.collect(productStats);
                            }
                        });

        //Step-7.1 关联SKU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSkuDS = AsyncDataStream.unorderedWait(productStatsDS,
                new DimAsyncFunction<ProductStats, ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        //这里传入字段名
                        return String.valueOf(input.getSku_id());
                    }

                    @Override//关联操作
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSku_name(dimInfo.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfo.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(dimInfo.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(dimInfo.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfo.getLong("TM_ID"));
                    }
                }, 60, TimeUnit.SECONDS);

        //Step-7.2 关联SPU维度
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSkuDS,
                new DimAsyncFunction<ProductStats, ProductStats>("DIM_SPU_INFO") {
                    @Override
                    public String getKey(ProductStats input) {
                        return input.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setSpu_name(dimInfo.getString("SPU_NAME"));
                    }
                }, 60, TimeUnit.SECONDS);

        //Step-7.3 关联品类维度
        SingleOutputStreamOperator<ProductStats> productStatsWithCategory3DS
                = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats, ProductStats>("DIM_BASE_CATEGORY3") {
                    @Override
                    public String getKey(ProductStats input) {
                        return String.valueOf(input.getCategory3_id());
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfo) throws ParseException {
                        productStats.setCategory3_name(dimInfo.getString("NAME"));
                    }
                }, 60, TimeUnit.SECONDS);


        //Step-7.4 补充品牌维度
        SingleOutputStreamOperator<ProductStats> productStatsWithTmDS =
                AsyncDataStream.unorderedWait(productStatsWithCategory3DS,
                        new DimAsyncFunction<ProductStats, ProductStats>("DIM_BASE_TRADEMARK") {
                            @Override
                            public void join(ProductStats productStats, JSONObject jsonObject) throws ParseException {
                                productStats.setTm_name(jsonObject.getString("TM_NAME"));
                            }

                            @Override
                            public String getKey(ProductStats productStats) {
                                return String.valueOf(productStats.getTm_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        productStatsWithTmDS.print(">>>to save");

        //Step-8 写入clickhouse
        productStatsWithTmDS.addSink(
                ClickHouseUtil.<ProductStats>getJdbcSink(
                        "insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));
        System.out.println("写入clickHouse成功！");
        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.dws;

import akka.japi.tuple.Tuple4;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.VisitorStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

/**
 * @ClassName gmall-flink-VisitorStatsApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日15:45 - 周一
 * @Describe 访客主题宽表计算
 * 对渠道、新老用户、app版本、省市区域这4个指标来聚合
 * 聚合窗口10秒
 */
public class VisitorStatsApp {
    public static void main(String[] args) throws Exception {

        //Step-1 声明环境 & 准备变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        //消费者组
        String groupId = "visitor_stats_app";
        //消费主题
        String pageViewSourceTopic = "dwd_page_log";
        String uniqueVisitSourceTopic = "dwm_unique_visit";
        String userJumpDetailSourceTopic = "dwm_user_jump_detail";

        //Step-2 从Kafka主题中接收消息
        FlinkKafkaConsumer<String> pageView = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        FlinkKafkaConsumer<String> uniqueVisit = MyKafkaUtil.getKafkaSource(uniqueVisitSourceTopic, groupId);
        FlinkKafkaConsumer<String> userJump = MyKafkaUtil.getKafkaSource(userJumpDetailSourceTopic, groupId);


        DataStreamSource<String> pageViewDStream = env.addSource(pageView);
        DataStreamSource<String> uniqueVisitDStream = env.addSource(uniqueVisit);
        DataStreamSource<String> userJumpDStream = env.addSource(userJump);

        //Step-3 将三条流转换为统一的VisitorStats格式再进行union
        //pv流
        SingleOutputStreamOperator<VisitorStats> pvDS = pageViewDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    JSONObject common = jsonObj.getJSONObject("common");
                    return new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 1L, 0L, 0L,
                            jsonObj.getJSONObject("page").getLong("during_time"),
                            jsonObj.getLong("ts"));
                }
        );

        //uv流
        SingleOutputStreamOperator<VisitorStats> uvDS = uniqueVisitDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    JSONObject common = jsonObj.getJSONObject("common");
                    return new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            1L, 0L, 0L, 0L, 0L,
                            jsonObj.getLong("ts"));
                }
        );

        //uj流
        /*
         * Explain 这里要注意!
         * 因为uj数据是经过了处理的,我们当时使用了within(10),也就是当数据同时过来时,一份pageLog数据直接进入到这里执行,但另一份
         * pageLog数据需要经过uj的处理才来union,但是uj那里需要10秒后才能返回数据,也就是这里的时间戳已经过去了10秒,窗口已经关闭,
         * 而uj那里还没有返回数据过来进行关联
         *
         *
         * 具体解释:https://www.bilibili.com/video/BV1Ju411o7f8?p=120&t=637.5
         * */
        SingleOutputStreamOperator<VisitorStats> ujDS = userJumpDStream.map(
                json -> {
                    JSONObject jsonObj = JSON.parseObject(json);
                    JSONObject common = jsonObj.getJSONObject("common");
                    return new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 0L, 1L, 0L,
                            jsonObj.getLong("ts"));
                }
        );

        //进入页面数流,统计每天访问页面的人数,跳转的肯定不算,Attention 注意sv_ct字段
        SingleOutputStreamOperator<VisitorStats> sessionVisitDS = pageViewDStream.process(new ProcessFunction<String, VisitorStats>() {
            @Override
            public void processElement(String value, ProcessFunction<String, VisitorStats>.Context ctx, Collector<VisitorStats> out) throws Exception {
                JSONObject jsonObj = JSON.parseObject(value);
                String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                JSONObject common = jsonObj.getJSONObject("common");
                //上一页为空就代表这是刚进入这个网页
                if (lastPageId == null || lastPageId.length() == 0) {
                    new VisitorStats("", "",
                            common.getString("vc"),
                            common.getString("ch"),
                            common.getString("ar"),
                            common.getString("is_new"),
                            0L, 0L, 1L, 0L, 0L,
                            common.getLong("ts"));
                }
            }
        });

        //Step-4 将四条流关联起来,这里关联,只是将4个小水管共同接着一根大水管,谁先来谁先进
        //这四条流内部有序,但是外部是乱序的,也就是pvDS可能在ujDS前打印出也可能在后,但内部是有序的
        DataStream<VisitorStats> unionDetailDS = uvDS.union(
                pvDS,
                sessionVisitDS,
                ujDS);

        //Step-5 给大水管中的数据打上时间戳水位线,这里取的是事件时间,延迟为11秒,因为要等待uj(处理时间11秒)的数据过来
        SingleOutputStreamOperator<VisitorStats> visitorStatsWithWatermarkDS =
                unionDetailDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<VisitorStats>forBoundedOutOfOrderness(Duration.ofSeconds(11))
                        .withTimestampAssigner(((element, ts) -> element.getTs())));

        //Step-6 分组,将大水管中分流
        /*
         * KeySelector<IN,KEY>
         * IN就是进入的元素,而KEY就是指定按什么进行分组,这里就是按照4个维度信息分组
         * */
        KeyedStream<VisitorStats, Tuple4<String, String, String, String>> visitorStatsTuple4KeyedStream = visitorStatsWithWatermarkDS.keyBy(
                new KeySelector<VisitorStats, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(VisitorStats value) throws Exception {
                        return new Tuple4<>(value.getVc(),//版本
                                value.getCh(),//渠道
                                value.getAr(),//地区
                                value.getIs_new());//是否新用户
                    }
                });

        //Step-7 开窗聚合,10秒滚动窗口,每个key只会聚合自己组的,但是水位线是共享的
        WindowedStream<VisitorStats, Tuple4<String, String, String, String>, TimeWindow> windowedStream = visitorStatsTuple4KeyedStream
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //Step-8 聚合相邻到来的值,第一个值不会进来
        SingleOutputStreamOperator<VisitorStats> Four2OneResult = windowedStream.reduce(
                new ReduceFunction<VisitorStats>() {
                    @Override
                    public VisitorStats reduce(VisitorStats stats1, VisitorStats stats2) throws Exception {
                        /*
                         * Attention
                         * 这里是聚合4个度量值
                         * 注意这里只有4个为key的维度字段是相同的,其余字段或多或少都是不同的,
                         * 若在滑动窗口中,只能使用new对象的方式返回,因为滑动窗口一个对象是要给多个窗口去调用的
                         * 所以推荐使用new VisitorStats(...)方式返回
                         * */
                        stats1.setPv_ct(stats1.getPv_ct() + stats2.getPv_ct());
                        stats1.setUv_ct(stats1.getUv_ct() + stats2.getUv_ct());
                        stats1.setUj_ct(stats1.getUj_ct() + stats2.getUj_ct());
                        stats1.setSv_ct(stats1.getSv_ct() + stats2.getSv_ct());
                        stats1.setDur_sum(stats1.getDur_sum() + stats2.getDur_sum());
                        return stats1;
                    }
                },
                /*
                 * Attention 这种写法将ReduceFunction和ProcessWindowFunction都当作参数传入
                 *  其结果是将经过ReduceFunction处理的数据,也就是每个10秒窗口不同key只有一条聚合值发送给ProcessWindowFunction窗口
                 *  进行再处理
                 * */
                new ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple4<String, String, String, String> tuple4, ProcessWindowFunction<VisitorStats, VisitorStats, Tuple4<String, String, String, String>, TimeWindow>.Context context, Iterable<VisitorStats> elements, Collector<VisitorStats> out) throws Exception {
                        for (VisitorStats element : elements) {//看似在做循环,其实里面只有一条聚合数据
                            //此窗口的起始时间戳
                            String startDate = DateTimeUtil.toYMDhms(new Date(context.window().getStart()));
                            //此窗口的结束时间戳
                            String endDate = DateTimeUtil.toYMDhms(new Date(context.window().getEnd()));
                            element.setStt(startDate);
                            element.setEdt(endDate);
                            out.collect(element);
                        }
                    }
                });

        /*
         * Q&A
         * Q1:Phoenix也是jdbc连接的,为什么不直接写JdbcSink呢?
         * A1:因为Phoenix中有多张表,每个表的字段数都不同,而ClickHouse中只存了一张表,字段数也是确定的,
         * 所以可以直接使用flink提供的jdbcSink
         *
         * Explain
         * JdbcSink就是典型的一流对一表,这里我们可以仿造JdbcSink.sink()的方式写一个自己的工具类为
         * ClickHouseUtil.sink,具体仿造法可以点进去sink中看,返回一个SinkFunction即可
         * */

        Four2OneResult.print(">>>");

        //Attention 注意这种insert写法必须顺序需要一样
        Four2OneResult.addSink(ClickHouseUtil.<VisitorStats>getJdbcSink(
                "insert into visitor_stats values(?,?,?,?,?,?,?,?,?,?,?,?)"
        ));

        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.DateTimeUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Date;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-ProvinceStatsApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月23日14:22 - 周四
 * @Describe 地区主题表(DataStream版)
 */
public class ProvinceStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String groupId = "province_stats";

        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(ORDER_WIDE_TOPIC, groupId));

        SingleOutputStreamOperator<ProvinceStats> provinceStatsDS = orderWideKafkaDS.map(x -> {
            OrderWide orderWide = JSON.parseObject(x, OrderWide.class);
            return new ProvinceStats(orderWide);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<ProvinceStats>forBoundedOutOfOrderness(Duration.ofSeconds(2))//延迟2秒
                .withTimestampAssigner((ele, record) -> {
                    return ele.getTs();
                }));

        //Step-3 开窗计算
        SingleOutputStreamOperator<ProvinceStats> reduceDS = provinceStatsDS.keyBy(ProvinceStats::getProvince_id)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<ProvinceStats>() {
                    @Override
                    public ProvinceStats reduce(ProvinceStats v1, ProvinceStats v2) throws Exception {
                        System.out.println("v1:" + v1.getOrder_count() + ",v2:" + v2.getOrder_count());
                        v1.setOrder_count(v1.getOrder_count() + v2.getOrder_count());
                        v1.setOrder_amount(v1.getOrder_amount().add(v2.getOrder_amount()));
                        return v1;
                    }
                }, new ProcessWindowFunction<ProvinceStats, ProvinceStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<ProvinceStats, ProvinceStats, Long, TimeWindow>.Context context, Iterable<ProvinceStats> elements, Collector<ProvinceStats> out) throws Exception {
                        ProvinceStats next = elements.iterator().next();
                        String startDate = DateTimeUtil.toYMDhms(new Date(context.window().getStart()));
                        String endDate = DateTimeUtil.toYMDhms(new Date(context.window().getEnd()));
                        next.setStt(startDate);
                        next.setEdt(endDate);
                        out.collect(next);
                    }
                });

        reduceDS.print();
        reduceDS.addSink(ClickHouseUtil.<ProvinceStats>
                getJdbcSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        env.execute();
    }
}

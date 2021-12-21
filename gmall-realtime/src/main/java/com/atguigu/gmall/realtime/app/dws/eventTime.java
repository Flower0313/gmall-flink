package com.atguigu.gmall.realtime.app.dws;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Iterator;

/**
 * @ClassName gmall-flink-eventTime
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日22:52 - 周一
 * @Describe
 */
public class eventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> hadoop102 = env.socketTextStream("hadoop102", 31313);

        SingleOutputStreamOperator<Tuple2<String, String>> streamOperator = hadoop102.map(x -> {
            String[] split = x.split(",");
            return new Tuple2<String, String>(split[0], split[1]);
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(2)
                ).withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, String>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, String> element, long recordTimestamp) {
                        return Long.parseLong(element.f1) * 1000L;
                    }
                }));

        //streamOperator.print();
        /*streamOperator.keyBy(x -> x.f0).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<Tuple2<String, String>, Tuple2<String, String>, String, TimeWindow>.Context context, Iterable<Tuple2<String, String>> elements, Collector<Tuple2<String, String>> out) throws Exception {
                Iterator<Tuple2<String, String>> iterator = elements.iterator();
                out.collect(iterator.next());
            }
        }).print();*/

        env.execute();
    }
}

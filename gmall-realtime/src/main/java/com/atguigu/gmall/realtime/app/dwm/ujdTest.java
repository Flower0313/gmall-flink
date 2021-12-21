package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @ClassName gmall-flink-ujdTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月16日16:52 - 周四
 * @Describe
 */
public class ujdTest {
    public static void main(String[] args) throws Exception {
//Step-1 准备环境 & 数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> sourceStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\pageLog.txt");
        //DataStreamSource<String> sourceStream = env.socketTextStream("hadoop102", 31313);


        //Step-2 将String转换为Json,并过滤脏数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = sourceStream.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObject = JSONObject.parseObject(value);
                            out.collect(jsonObject);
                        } catch (Exception e) {
                            //过滤脏数据
                            ctx.output(new OutputTag<String>("dirty") {
                            }, value);
                        }
                    }
                })//抽取ts字段当成事件时间的时间戳
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts") * 1000L;
                            }
                        }));
        //按照mid分组
        KeyedStream<JSONObject, String> keyedStream = kafkaDS
                .keyBy(x -> x.getJSONObject("common").getString("mid"));


        //注册模式,模式会应用在每个不同的mid组上
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("begin")
                .where(new SimpleCondition<JSONObject>() {
                    @Override//筛选出访问页面的数据
                    public boolean filter(JSONObject value) throws Exception {
                        String lastPage = value.getJSONObject("page").getString("last_page_id");
                        //System.out.println(lastPage == null || lastPage.length() <= 0);
                        //System.out.println(value);
                        return lastPage == null || lastPage.length() <= 0;
                    }
                })
                .times(2)//选出两次访问主页的动作
                .consecutive()//分组内严格连续
                .within(Time.seconds(5));//选出两个连续访问页面10秒之内的,没有满足此模式的数据都进入到了超时侧输出流
        //在5秒内的窗口中需要等到两个满组lastPage==null的数据

        //将模式作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //定义超时标记
        OutputTag<JSONObject> timeOutTag = new OutputTag<JSONObject>("TimeOut") {
        };


        SingleOutputStreamOperator<JSONObject> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject timeout(Map<String, List<JSONObject>> map, long l) throws Exception {

                        //提取事件，若只有一条唯一数据会进入到超时输出流
                        return map.get("begin").get(0);
                    }
                },
                new PatternSelectFunction<JSONObject, JSONObject>() {
                    @Override
                    public JSONObject select(Map<String, List<JSONObject>> map) throws Exception {

                        //提取事件
                        return map.get("begin").get(0);
                    }
                });
        DataStream<JSONObject> timeOutDS = selectDS.getSideOutput(timeOutTag);
        //timeOutDS.print("超时数据>>");
        DataStream<JSONObject> unionDS = selectDS.union(timeOutDS);
        unionDS.print("正常数据>>");
        //unionDS.print();

        env.execute();
    }
}

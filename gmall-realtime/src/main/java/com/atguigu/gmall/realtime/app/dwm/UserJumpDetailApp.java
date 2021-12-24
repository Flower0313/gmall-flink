package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-UserJumpDetailApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月16日13:59 - 周四
 * @Describe 跳出明细计算：跳出就是用户成功访问了网站的一个页面后就退出，不在继续访问网站的其它页面。而跳出率就是用跳出次数除以访问次数。
 * <p>
 * 数据来源:dwd_page_log
 * 数据去向:dwm_user_jump_detail
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境 & 数据
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2  读取Kafka dwd_page_log主题数据创建流
        String groupId = "userJumpDetailApp";

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(PAGE_LOG_TOPIC, groupId);
        DataStreamSource<String> sourceDS = env.addSource(kafkaSource);

        //DataStream<String> sourceStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\pageLog.txt");


        //Step-2 将String转换为Json,并过滤脏数据
        SingleOutputStreamOperator<JSONObject> kafkaDS = sourceDS.process(new ProcessFunction<String, JSONObject>() {
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
                                return element.getLong("ts");
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
                        return lastPage == null || lastPage.length() <= 0;
                    }
                })
                .times(2)//选出两次访问主页的动作
                .consecutive()//分组内严格连续,加上后等同于.where().next().where()的写法
                .within(Time.seconds(10));//选出两个连续访问页面10秒之内的,没有满足此模式的数据都进入到了超时侧输出流
        /*
         * Q&A
         * Q1:这里为什么需要加within(10)呢?10是自己的需求
         * A1:因为若直接不加时间，那么这就是严格连续的时间，都没有容忍的时间，其实超时数据和正常数据都是求的跳出的数据;
         * 输出条件1：若一条null数据到了，但下一条数据超过了10秒，它10秒后输出，它算一次跳出数据，因为你10秒后再做操作就相当于跳出了
         * 输出条件2：若一条null数据来了，在10秒内又来了一条null数据，它立即输出，也算跳出数据，因为相当于第一条啥都没干就跳走了
         * 所以加了within只是输出的条件不一样，结果还是一样的，不同的是输出的数据时间点，满足任意输出条件即可。
         * */

        //将模式作用在流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);
        //定义超时标记
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut") {
        };

        SingleOutputStreamOperator<String> selectDS = patternStream.select(timeOutTag,
                new PatternTimeoutFunction<JSONObject, String>() {
                    @Override//超时事件
                    public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                        //Map中的key就是模式组的名称
                        List<JSONObject> begin = pattern.get("begin");
                        return begin.get(0).toString();
                    }
                }, new PatternSelectFunction<JSONObject, String>() {
                    @Override//正常事件
                    public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                        List<JSONObject> begin = pattern.get("begin");
                        //get(1)就是匹配上后面那条数据,若不懂可以去看FlinkDemo中的cep的fucker测试类
                        return begin.get(0).toString();
                    }
                });

        DataStream<String> userJumpDetailDS = selectDS.getSideOutput(timeOutTag);
        //userJumpDetailDS.print("条件1跳出数据>>>");

        DataStream<String> result = selectDS.union(userJumpDetailDS);
        //result.print("组合条件数据>>>");
        //输入到kafka
        result.print(">>>");
        result.addSink(MyKafkaUtil.getKafkaSink(USER_JUMP_TOPIC));
        env.execute();
    }
}

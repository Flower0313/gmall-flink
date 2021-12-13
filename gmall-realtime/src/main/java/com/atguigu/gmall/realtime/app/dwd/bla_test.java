package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.CommonEnv;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;

/**
 * @ClassName gmall-flink-bla_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日20:09 - 周一
 * @Describe
 */
public class bla_test {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 准备数据源
        DataStreamSource<String> sourceStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\applogs.txt");

        /*
         * Step-3 将string格式的json 转换为 json格式的json
         * Q&A!
         * Q1:为啥actions的k-value键值对不见了
         * A1:因为这个json格式转换只获取{}中的数据,不管[]中的
         *
         * Q2:这里为什么不直接用map将string转换为json呢?
         * A2:因为怕数据中有脏数据引起程序异常,异常程序就写入到侧输出流
         * */
        SingleOutputStreamOperator<JSONObject> jsonObjDS = sourceStream.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    out.collect(JSONObject.parseObject(value));
                } catch (Exception e) {
                    //若读取到了脏数据,就将脏数据写入到侧输出流
                    ctx.output(new OutputTag<String>("Dirty") {
                    }, value);
                }
            }
        });

        //Step-4 数据处理逻辑
        //4.1按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(data -> data.getJSONObject("common").getString("mid"));

        //4.2 使用状态做新老用户校验
        DataStream<JSONObject> jsonWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //声明状态用于表示当前Mid是否已经访问过,没有相同mid共用一个first状态
            private ValueState<Boolean> firstVisitDataState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //在生命周期上下文开启后注册状态
                firstVisitDataState = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("is_new", Boolean.class));
            }

            @Override
            public void close() throws Exception {
                //清空状态
                firstVisitDataState.clear();
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //取出三种日志中公共的is_new字段
                String is_new = value.getJSONObject("common").getString("is_new");
                //判断用户是否为真正的新用户
                if ("1".equals(is_new)) {
                    Boolean isFirst = firstVisitDataState.value();
                    Long ts = value.getLong("ts");
                    if (isFirst != null) {
                        //进入到这里表示不是第一次添加进来,这不是新用户,将标识改为老用户
                        value.getJSONObject("common").put("is_new", "0");
                    } else {
                        //若进入到这里,表示此mid用户是新用户,然后就将状态更新为true,下次相同的mid进来就是老用户了
                        firstVisitDataState.update(true);
                    }
                }
                return value;
            }
        });

        //4.3 使用侧输出流实现三种日志的分流,进入侧输出流的数据就不会在主流输出了
        SingleOutputStreamOperator<String> pageDS = jsonWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                //提取出start字段
                JSONObject isStart = value.getJSONObject("start");
                //若含有start字段并且长度大于0的就是启动日志
                if (isStart != null && isStart.size() > 0) {
                    //explain 输出到start侧输出流
                    ctx.output(new OutputTag<String>("start") {
                    }, value.toString());
                } else {
                    //将页面数据输出到主流
                    out.collect(value.toString());
                    //Attention 注意这里是JSONArray
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        //取出一个用户的一系列动作,再拼接上page_id
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject object = displays.getJSONObject(i);
                            object.put("page_id", value.getJSONObject("page").getString("page_id"));
                            //explain 输出到page侧输出流
                            ctx.output(new OutputTag<String>("display") {
                            }, object.toString());
                        }
                    }
                }
            }
        });

        //jsonWithNewFlagDS.print();
        //打印出三个流
        pageDS.print("Pages>>>");
        pageDS.getSideOutput(new OutputTag<String>("Start") {
        }).print("Start>>>");
        pageDS.getSideOutput(new OutputTag<String>("Display") {
        }).print("Display>>>");
        jsonObjDS.getSideOutput(new OutputTag<String>("Dirty"){}).print("Dirty>>>");


        //Step-5 执行flink任务
        env.execute();
    }
}

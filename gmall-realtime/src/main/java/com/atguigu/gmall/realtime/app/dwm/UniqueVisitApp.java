package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.eclipse.jetty.util.ajax.JSON;

import java.text.SimpleDateFormat;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-UniqueVisitAp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月16日10:08 - 周四
 * @Describe 需求:统计每日每一个用户第一次访问的数据
 * 注意点1:识别出该访客打开的第一个页面,表示这个访客开始进入我们的应用
 * 2.访客一天内可能多次进入应用,应在一天范围内去重
 * 数据来源:dwd_page_log
 * 数据去向:dwm_unique_visit
 */
public class UniqueVisitApp {
    private static KeyedStream<JSONObject, String> midStream;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStreamSource<String> sourceStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\pageLog.txt");
        //Step-2 接收kafka传来的数据
        String groupId = "unique_visit_app";//消费者主题

        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(PAGE_LOG_TOPIC, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);


        //Step-3 将String转换为Json,并过滤脏数据
        SingleOutputStreamOperator<JSONObject> process = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    //过滤脏数据,转换成json失败就是脏数据
                    ctx.output(new OutputTag<String>("dirty") {
                    }, value);
                }
            }
        });

        //Step-4 按mid分组
        KeyedStream<JSONObject, String> midStream = process.keyBy(x -> x.getJSONObject("common").getString("mid"));

        //Step-5 过滤 & 去重(状态编程)
        SingleOutputStreamOperator<JSONObject> filterDS = midStream.filter(new RichFilterFunction<JSONObject>() {
            //Attention
            //声明状态,每个key都有自己的firstVisitState状态!!互不影响
            private ValueState<String> firstVisitState;
            private SimpleDateFormat sdf;

            @Override
            public void open(Configuration parameters) throws Exception {
                //注册状态
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("first", String.class);
                firstVisitState = getRuntimeContext().getState(stateDescriptor);

                //按天切割,每个时间戳都会按天来算
                sdf = new SimpleDateFormat("yyyy-MM-dd");

                //Attention 定义状态有效期
                StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.days(1))//数据的有效期,定义每天清空一次
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)//仅在创建和写入时更新,意思是每次状态更新的时候就会重新计算1天的清空倒计时
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);

            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出上一次访问页面
                String lastPageId = value.getJSONObject("page").getString("last_page_id");

                //判断是否存在上一个页面,若不存在则表示一次访问
                if (lastPageId == null || lastPageId.length() <= 0) {
                    String firstDate = firstVisitState.value();
                    String curDate = sdf.format(value.getLong("ts"));
                    /*
                     * Explain
                     *  我们这里只对每天的相同用户去重
                     *  若时间状态为null则表示此次访问是第一次访问 || 若进来的日期与已存状态日期不相等,则说明是不同天的数据了
                     * */
                    if (firstDate == null || !firstDate.equals(curDate)) {
                        firstVisitState.update(curDate);
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    //若存在上一次页面,则证明这不是一次访问,而是应用内跳转,直接返回false
                    return false;
                }
            }
        });

        //Step-6 打印出过滤数据
        filterDS.map(JSON::toString).addSink(MyKafkaUtil.getKafkaSink(UNIQUE_VISIT_TOPIC));
        filterDS.print("uv>>>>");
        env.execute();
    }
}

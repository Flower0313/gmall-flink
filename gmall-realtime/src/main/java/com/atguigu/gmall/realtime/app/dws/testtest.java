package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-testtest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日23:16 - 周五
 * @Describe
 */
public class testtest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //消费者组
        String groupId = "visitor_stats_app";


        //Step-2 从Kafka主题中接收消息
        DataStreamSource<String> pageViewDStream = env
                .addSource(MyKafkaUtil.getKafkaSource(PAGE_LOG_TOPIC, groupId));
        DataStreamSource<String> uniqueVisitDStream = env
                .addSource(MyKafkaUtil.getKafkaSource(UNIQUE_VISIT_TOPIC, groupId));
        DataStreamSource<String> userJumpDStream = env
                .addSource(MyKafkaUtil.getKafkaSource(USER_JUMP_TOPIC, groupId));

        pageViewDStream.print("pv>>");
        uniqueVisitDStream.print("uv>>");
        userJumpDStream.print("uj>>");

        env.execute();
    }
}

package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @ClassName gmall-flink-MyKafkaUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日17:57 - 周一
 * @Describe 获取kafka的producer生产者连接器
 * 官方文档:https://nightlies.apache.org/flink/flink-docs-release-1.13/zh/docs/connectors/datastream/kafka/
 */
public class MyKafkaUtil {
    private static String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    private static Properties properties = new Properties();

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    }

    /**
     * @param topic kafka主题
     * @return flink的连接kafka生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(), //序列化schema
                properties //producer配置
        );
    }

    /**
     * @param topic kafka主题
     * @param groupId 消费者组
     * @return flink的连接kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //给配置信息添加消费组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);

    }
}

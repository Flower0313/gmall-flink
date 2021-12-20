package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
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
    private static String DEFAULT_TOPIC = "dwd_default_topic";
    private static Properties properties = new Properties();

    static {
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
    }

    /**
     * @param topic kafka主题,消息是以String类型写入kafka
     * @return flink的连接kafka生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic,
                new SimpleStringSchema(), //序列化schema
                properties //producer配置
        );
    }

    /**
     * @param topic   kafka主题
     * @param groupId 消费者组
     * @return flink的连接kafka消费者对象
     */
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        //给配置信息添加消费组
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), properties);
    }

    //泛型T根据参数中传入的T类型来决定,因为你传入kafka的数据不一定总是String,写死不灵活
    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, // 第一个参数不能为空,随便填写什么都行
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}

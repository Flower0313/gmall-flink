package com.atguigu.gmall.realtime.app.dwm;

import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @ClassName gmall-flink-OrderWideApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日19:25 - 周五
 * @Describe 读取订单和订单明细数据
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        //2.读取Kafka订单和订单明细主题数据 dwd_order_info  dwd_order_detail
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //Step-2.1 接收订单数据信息OrderInfo
        FlinkKafkaConsumer<String> orderInfoKafkaSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoDS = env.addSource(orderInfoKafkaSource);

        //Step-2.2 接收订单明细数据OrderDetail
        FlinkKafkaConsumer<String> orderDetailKafkaSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailKafkaDS = env.addSource(orderDetailKafkaSource);

        //Step-3 打印
        //orderInfoDS.print("orderInfo");
        orderDetailKafkaDS.print("orderDetail");

        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderWide;
import com.atguigu.gmall.realtime.bean.PaymentInfo;
import com.atguigu.gmall.realtime.bean.PaymentWide;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.windowing.time.Time;


import java.text.ParseException;
import java.text.SimpleDateFormat;

/**
 * @ClassName gmall-flink-PaymentWideApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日11:05 - 周一
 * @Describe
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-1 声明Kafka主题
        String groupId = "payment_wide_group";
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";

        //Step-2 获取数据流
        //支付表,直接从BaseDBApp中采集过来
        // DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        DataStreamSource<String> paymentKafkaDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\paymentInfo.txt");

        DataStreamSource<String> orderWideKafkaDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderwide.txt");

        //订单宽表,还有进行关联等一系列处理,时间肯定更长
        //DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));

        //Step-3 将数据转为JavaBean并提取时间戳生成waterMark
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentKafkaDS.map(x -> JSON.parseObject(x, PaymentInfo.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            try {
                                return sdf.parse(element.getCreate_time()).getTime();
                            } catch (ParseException e) {
                                throw new RuntimeException("时间格式错误");
                            }
                        })));

        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideKafkaDS
                .map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forMonotonousTimestamps()
                        .withTimestampAssigner(((element, recordTimestamp) -> {
                            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            try {
                                return sdf.parse(element.getCreate_time()).getTime();
                            } catch (ParseException e) {
                                throw new RuntimeException("时间格式错误");
                            }
                        })));

        //Step-4 按照orderId双流join
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoDS.keyBy(PaymentInfo::getOrder_id)
                .intervalJoin(orderWideDS.keyBy(OrderWide::getOrder_id))
                /*
                 * Explain:为什么lowerBound设置保留15分钟?upperBound设置为5分钟?
                 *  程序逻辑:你先下订单,所以程序一开始先有orderWide,然后用户在15分钟选择付款,所以若不考虑网络延迟和
                 *  orderWide处理时间的话,paymentInfo是一定比orderWide晚来的,这里就需要保留orderWide的数据
                 *  15分钟,但考虑到以下这种情况:
                 *  一个用户在下单之后立马就付款,且由于网络传输和处理时间的原因导致paymentInfo比orderWide先来了,
                 *  所以这时候就要设置paymentInfo保留5分钟。
                 * */
                .between(Time.minutes(-15), Time.seconds(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        paymentInfoDS.map(JSON::toJSONString).print(">>>paymentWide");
        /*
        * 数据格式：
        * {"callback_time":"2020-12-21 19:30:46","create_time":"2020-12-21 23:28:49","id":2,"order_id":26454,
        * "payment_type":"1102","subject":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦
        * 6GB+128GB亮黑色全网通5G手机等7件商品","total_amount":42952.00,"user_id":13}
        * */

        //Step-5 将JavaBean类转为Json格式再发送给kafka类型
        paymentInfoDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}

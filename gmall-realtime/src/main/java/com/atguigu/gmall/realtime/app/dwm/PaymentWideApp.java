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
        env.setParallelism(2);

        //Step-1 声明Kafka主题
        String groupId = "payment_wide_group";
        //数据来源
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        //数据去向
        String paymentWideSinkTopic = "dwm_payment_wide";

        //Step-2 获取数据流
        //DataStreamSource<String> paymentKafkaDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\paymentInfo.txt");
        //DataStreamSource<String> orderWideKafkaDS = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderwide.txt");

        //支付表,直接从BaseDBApp中采集过来
        DataStreamSource<String> paymentKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId));
        //订单宽表,还有进行关联等一系列处理,时间肯定更长
        DataStreamSource<String> orderWideKafkaDS = env.addSource(MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId));


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
                 *  一个用户在下单之后立马就付款,且由于网络传输和OrderWide处理时间的原因导致paymentInfo比orderWide先来了,
                 *  所以这时候就要设置paymentInfo保留5分钟。
                 * */
                .between(Time.minutes(-15), Time.minutes(5))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo left, OrderWide right, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) throws Exception {
                        out.collect(new PaymentWide(left, right));
                    }
                });

        /*
         * 数据格式：
         * {"activity_reduce_amount":0.00,"callback_time":"2020-12-21 19:30:46","category3_id":477,"category3_name":
         * "唇部","coupon_reduce_amount":0.00,"detail_id":79382,"feight_fee":8.00,"order_create_time":"2020-12-21
         * 23:28:49","order_id":26454,"order_price":129.00,"order_status":"1001","original_total_amount":336.00,
         * "payment_create_time":"2020-12-21 23:28:49","payment_id":2,"payment_type":"1102",
         * "province_3166_2_code":"ISO_3166_2","province_area_code":"530000","province_id":33,"province_iso_code":
         * "CN-53","province_name":"云南","sku_id":27,"sku_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 Z02少女红
         * 活力青春 璀璨金钻哑光唇膏 ","sku_num":1,"split_activity_amount":0.0,"split_coupon_amount":0.0,
         * "split_total_amount":129.00,"spu_id":9,"spu_name":"索芙特i-Softto 口红不掉色唇膏保湿滋润 璀璨金钻哑光唇膏 ",
         * "subject":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万超感知徕卡三摄 30倍数字变焦 6GB+128GB亮黑色全网通5G手机等7件商品
         * ","tm_id":8,"tm_name":"索芙特","total_amount":344.00,"user_age":31,"user_gender":"M","user_id":26}
         * */

        //Step-5 将JavaBean类转为Json格式再发送给kafka类型
        //paymentWideDS.map(JSON::toJSONString).print();
        paymentWideDS.map(JSON::toJSONString)
                .addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}

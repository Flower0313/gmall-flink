package com.atguigu.gmall.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.OrderDetail;
import com.atguigu.gmall.realtime.bean.OrderInfo;
import com.atguigu.gmall.realtime.bean.OrderWide;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

/**
 * @ClassName gmall-flink-owaTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日20:13 - 周五
 * @Describe
 */
public class owaTest {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 读取数据源
        DataStream<String> orderInfoStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderInfo.txt");

        DataStreamSource<String> orderDetailStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\orderdetail.txt");

        //Step-3 将String类型的JSON变成Java类
        //OrderInfo
        DataStream<OrderInfo> orderInfoDS = orderInfoStream.map(x -> {
                    OrderInfo orderInfo = JSON.parseObject(x, OrderInfo.class);
                    String create_time = orderInfo.getCreate_time();
                    //create_time的格式2021-03-14 20:04:25
                    String[] dateTimer = create_time.split(" ");
                    orderInfo.setCreate_date(dateTimer[0]);
                    orderInfo.setCreate_hour(dateTimer[1].split(":")[0]);
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderInfo.setCreate_ts(sdf.parse(create_time).getTime());//返回createTime对应的时间戳
                    return orderInfo;
                })//打上事件水位线
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderInfo>forMonotonousTimestamps()
                        .withTimestampAssigner((e, r) -> {
                            return e.getCreate_ts();
                        }));

        //OrderDetail
        DataStream<OrderDetail> orderDetailDS = orderDetailStream.map(x -> {
                    OrderDetail orderDetail = JSON.parseObject(x, OrderDetail.class);
                    String create_time = orderDetail.getCreate_time();
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    orderDetail.setCreate_ts(sdf.parse(create_time).getTime());//返回createTime对应的时间戳
                    return orderDetail;
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<OrderDetail>forMonotonousTimestamps()
                        .withTimestampAssigner((e, r) -> {
                            return e.getCreate_ts();
                        }));


        //Step-4 双流join
        SingleOutputStreamOperator<OrderWide> orderWideWithoutDim = orderInfoDS.keyBy(OrderInfo::getId)
                .intervalJoin(orderDetailDS.keyBy(OrderDetail::getOrder_id))
                /*
                 * Explain
                 *  生产环境中给最大延迟时,按道理来说orderInfo和orderDetail数据应该是同时来的,因为一个订单嘛,
                 *  但是难免网络传输问题会导致有一方落后,所以我们这里上下将数据保留5秒
                 * */
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) throws Exception {
                        out.collect(new OrderWide(left, right));
                    }
                });

        //Step-5 关联维度信息 Hbase Phoenix


        orderWideWithoutDim.print("orderWideWithoutDim");

        env.execute();

        /*
         * Explain orderWideWithoutDim的数据格式:
         * OrderWide(detail_id=79384, sku_id=29, order_price=69.00, sku_num=2, sku_name=CAREMiLLE珂曼奶油小方口红 雾面滋润保湿持久丝缎
         * 唇膏 M01醉蔷薇, split_activity_amount=0.0, split_coupon_amount=0.0, split_total_amount=138.00, province_id=33,
         * order_status=1001, user_id=26, order_id=26454, total_amount=344.00, activity_reduce_amount=0.00,
         * coupon_reduce_amount=0.00, original_total_amount=336.00, feight_fee=8.00, create_date=2020-12-21, create_hour=23,
         * create_time=2020-12-21 23:28:49, split_feight_fee=null, expire_time=null, operate_time=null, province_name=null,
         * province_area_code=null, province_iso_code=null, province_3166_2_code=null, user_age=null, user_gender=null,
         * spu_id=null, tm_id=null, category3_id=null, spu_name=null, tm_name=null, category3_name=null)
         * */
    }
}

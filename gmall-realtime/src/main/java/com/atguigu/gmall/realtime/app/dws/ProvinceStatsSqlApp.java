package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.bean.ProvinceStats;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static com.atguigu.gmall.realtime.common.CommonEnv.ORDER_DETAIL_TOPIC;
import static com.atguigu.gmall.realtime.common.CommonEnv.ORDER_WIDE_TOPIC;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName gmall-flink-ProvinceStatsSqlApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月22日20:26 - 周三
 * @Describe 地区主题表
 * 数据来源:dwd_order_wide
 * 数据去向:
 */
public class ProvinceStatsSqlApp {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        //Attention 若参数二不传值默认调用批环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        //定义消费者组
        String groupId = "province_stats";

        /*
         * Attention
         *  请注意,kafka传递过来的字段名要和这里的字段名一样,数据就能自动匹配上,多了的不匹配,少了为null
         *  这里create_time只能接受yyyy-MM-dd HH:mm:ss这种格式的数据
         *  查询语句中窗口必须写在group by中,因为按窗口来分组
         * */
        tableEnv.executeSql("create table order_wide(" +
                "`province_id` BIGINT," +
                "`province_name` STRING," +
                "`province_area_code` STRING," +
                "`province_iso_code` STRING," +
                "`province_3166_2_code` STRING," +
                "`order_id` BIGINT," +
                "`total_amount` DOUBLE," +
                "`create_time` timestamp(3)," + //给这个字段打上水位线,延迟时间未2秒
                "WATERMARK FOR create_time AS create_time - INTERVAL '2' SECOND" +
                MyKafkaUtil.getKafkaDDL(ORDER_WIDE_TOPIC, groupId));


        /*
         * Explain
         *  开一个10秒的滑动窗口,延迟时间为2秒,这里的字段和JavaBean字段顺序不重要,重要的是名称要一致
         *  通过count()和sum()函数聚合10秒内的聚合值
         *  需要直接查询出的字段都需要写在group by中,其中窗口是必须写在group by中的,因为我们的逻辑也是按窗口分组
         * */
        Table provinceStateTable = tableEnv.sqlQuery("select " +
                "DATE_FORMAT(TUMBLE_START(create_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as stt," +
                "DATE_FORMAT(TUMBLE_END(create_time, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as edt," +
                "province_id," +
                "province_name," +
                "province_area_code," +
                "province_iso_code," +
                "province_3166_2_code," +
                "sum(total_amount) order_amount," +
                "COUNT(DISTINCT order_id) order_count, " +
                "UNIX_TIMESTAMP()*1000 ts " +
                "from order_wide group by " +
                "TUMBLE(create_time, INTERVAL '10' SECOND)," +
                "province_id,province_name,province_area_code,province_iso_code,province_3166_2_code");


        //Attention 每个窗口中都是一个聚合值,这里直接输出就行,因为下个窗口也用不到上个窗口的值,所以不用撤回流
        DataStream<ProvinceStats> provinceStatsDataStream = tableEnv
                .toAppendStream(provinceStateTable, ProvinceStats.class);
        provinceStatsDataStream.addSink(ClickHouseUtil.<ProvinceStats>
                getJdbcSink("insert into province_stats values(?,?,?,?,?,?,?,?,?,?)"));

        provinceStatsDataStream.print();
        env.execute();
    }
}

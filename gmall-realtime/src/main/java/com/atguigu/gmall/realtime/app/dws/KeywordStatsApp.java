package com.atguigu.gmall.realtime.app.dws;

import com.atguigu.gmall.realtime.app.udf.KeyWordUDTF;
import com.atguigu.gmall.realtime.bean.KeywordStats;
import com.atguigu.gmall.realtime.common.GmallConstant;
import com.atguigu.gmall.realtime.utils.ClickHouseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static com.atguigu.gmall.realtime.common.CommonEnv.ORDER_WIDE_TOPIC;
import static com.atguigu.gmall.realtime.common.CommonEnv.PAGE_LOG_TOPIC;

/**
 * @ClassName gmall-flink-KeywordStatsApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月23日16:36 - 周四
 * @Describe 分词统计表
 * SQL中的数据类型:https://nightlies.apache.org/flink/flink-docs-release-1.12/zh/dev/table/types.html
 * 自定义函数:https://nightlies.apache.org/flink/flink-docs-release-1.14/zh/docs/dev/table/functions/udfs/
 * <p>
 * 数据来源:dwd_page_log
 * 数据去向:clickhouse:
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 1.定义Table流环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        //TODO 2.注册自定义函数进环境
        tableEnv.createTemporarySystemFunction("split_word", KeyWordUDTF.class);

        //TODO 3.将数据源定义为动态表
        String groupId = "keyword_stats_app";

        /*
         *  Attention
         *   from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')其中ts必须是10位的时间戳,
         *   因为flink中没有直接的timestamp的类型,所以要用bigint来转换
         *   13位的时间戳是会报错的,所以要除1000,所以这时候要BIGINT类型
         *   这里的MAP<STRING,STRING>是kv键值对类型的,因为common中数据格式是{"k1":"v1","k2":"v2"}
         *   所以这里用MAP类型,page['item']是可能为空的,若page['item']为null是不会输出的
         * */
        tableEnv.executeSql("create table page_view (" +
                "common MAP<STRING,STRING>," +
                "page MAP<STRING,STRING>," +
                "ts BIGINT," +
                "rt as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
                "watermark for rt as rt - interval '1' second" +
                MyKafkaUtil.getKafkaDDL(PAGE_LOG_TOPIC, groupId));

        //TODO 4.数据过滤,where后面是过滤条件,为上一跳页面为"search"和搜索词 is not null

        Table fullWordTable = tableEnv.sqlQuery("select " +
                "page['item'] full_word," +
                "rt from page_view " +
                "where page['item'] is not null " +
                "and page['last_page_id']='search'");


        Table keywordTable = tableEnv.sqlQuery("select " +
                "word," + //这里的word就是KeyWordUDTF中的输出类型,名称要一致
                "rt from " +
                fullWordTable +
                ",lateral table(split_word(full_word))" //attention 将full_word传递到自定义函数中
        );


        //TODO 5.使用自定义的函数
        Table keywordStatsSearch = tableEnv.sqlQuery("select " +
                "word," +
                "count(*) ct, " +
                "'" + GmallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rt, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from " + keywordTable
                + " GROUP BY TUMBLE(rt, INTERVAL '10' SECOND ),word");//attention 按照窗口和id分组

        //TODO 8.将表转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(keywordStatsSearch, KeywordStats.class);

        //TODO 7.写入到clickHouse
        keywordStatsDataStream.addSink(
                ClickHouseUtil.<KeywordStats>getJdbcSink(
                        "insert into keyword_stats(word,ct,source,stt,edt,ts)  " +
                                " values(?,?,?,?,?,?)"));

        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.CustomerDesrialization;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-Flink_CDCWithCustomerSchema
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日18:08 - 周一
 * @Describe
 */
public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 配置连接
        DebeziumSourceFunction<String> mysql = MySQLSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(DATABASE)
                .deserializer(new CustomerDesrialization())
                .startupOptions(StartupOptions.latest())//只消费新数据,不打印历史数据
                .build();

        //Step-3 连接source数据源
        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        //Step-4 将数据写入到kafka
        mysqlDS.print();
        mysqlDS.addSink(MyKafkaUtil.getKafkaSink(ODS_LOG_TOPIC));
        env.execute();
    }
}

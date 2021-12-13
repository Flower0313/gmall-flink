package com.atguigu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-DiySqlCDC
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日11:23 - 周一
 * @Describe 自定义flink cdc格式
 */
public class DiySqlCDC {
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
                .tableList(TABLE)
                .deserializer(new CustomerDesrialization())
                .build();

        //Step-3 连接source数据源
        DataStreamSource<String> mysqlDS = env.addSource(mysql);

        //Step-4 打印
        mysqlDS.print();
        env.execute();
    }
}

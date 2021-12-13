package com.atguigu.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Struct;
import java.util.Properties;

import static com.atguigu.common.CommonEnv.*;
import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName gmall-flink-SqlCDC
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日8:47 - 周一
 * @Describe com.atguigu.cdc.SqlCDC
 */
public class SqlCDC {
    public static void main(String[] args) throws Exception {
        //Step-1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        tableEnv.executeSql("CREATE TABLE trademarks (" +
                "  id STRING," +
                "  tm_name STRING," +
                "  logo_url STRING" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'root'," +
                "'database-name'='gmall_flink'," +
                "'table-name'='base_trademark'" +
                ")"
        );
        tableEnv.executeSql("select * from trademarks").print();


        //Step-2 创建mysql-cdc的source
        /*Properties properties = new Properties();

        MySQLSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(DATABASE)
                .tableList(TABLE)
                .deserializer(new DebeziumDeserializationSchema<String>())
                .build();*/

        env.execute();
    }
}

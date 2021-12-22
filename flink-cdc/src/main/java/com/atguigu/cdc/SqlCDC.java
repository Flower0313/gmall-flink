package com.atguigu.cdc;

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

        //Step-2 在本地创建一个trademarks表去接收base_trademark的数据,字段要对应
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

        //Step-3 查询本地表
        tableEnv.executeSql("select * from trademarks").print();

    }
}

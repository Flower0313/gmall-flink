package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.CustomerDesrialization;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-bda_test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日9:01 - 周二
 * @Describe
 */
public class bda_test {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 准备数据源
        DataStreamSource<String> sourceStream = env.readTextFile("T:\\ShangGuiGu\\gmall-flink\\gmall-realtime\\src\\main\\resources\\appdb.txt");

        //将string格式的json数据转换为json格式的json数据,过滤空值,也就是过滤掉delete操作的数据
        SingleOutputStreamOperator<JSONObject> filterDS = sourceStream.map(JSONObject::parseObject)
                .filter(value -> {
                    return !"delete".equals(value.getString("type"));
                });

        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(REAL_DATABASE).tableList(REAL_DATABASE + ".table_process")
                .deserializer(new CustomerDesrialization())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);
        tableProcessDS.print();

        //打印测试
        //filterDS.print();
        env.execute();
    }
}

package com.atguigu.gmall.realtime.app.ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.utils.CustomerDesrialization;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-GmallRealTime
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月17日22:32 - 周五
 * @Describe
 */
public class GmallRealTime {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 配置连接
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(REAL_DATABASE).tableList(REAL_DATABASE + ".table_process")
                .deserializer(new CustomerDesrialization())
                .startupOptions(StartupOptions.initial())//不开启会导致测试的时候广播流中没有key+type值,导致主流认为hbase中没有那个表
                .build();

        //Step-3 连接source数据源
        DataStreamSource<String> mysqlDS = env.addSource(sourceFunction);

        //Step-4 将mysql中变换的数据写入到kafka的obs_base_db

        mysqlDS.filter(x -> {//筛选不是delete操作的数据
            JSONObject jsonObject = JSONObject.parseObject(x);
            return !"delete".equals(jsonObject.getString("type"));
        }).print();

        env.execute();
        /*
        * Attention 数据类型
        * {"database":"gmall_realtime","before":{},"after":{"operate_type":"update","sink_type":"hbase",
        * "sink_table":"dim_user_info","source_table":"user_info",
        * "sink_columns":"id,login_name,name,user_level,birthday,gender,create_time,operate_time"},
        * "type":"insert","table":"table_process"}
        * */
    }
}

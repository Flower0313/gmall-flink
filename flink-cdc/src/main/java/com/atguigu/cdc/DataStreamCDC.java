package com.atguigu.cdc;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import static com.atguigu.common.CommonEnv.*;

/**
 * @ClassName gmall-flink-FlinkCDC
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月12日18:37 - 周日
 * @Describe com.atguigu.cdc.DataStreamCDC
 */
public class DataStreamCDC {
    public static void main(String[] args) throws Exception {
        //Step-1.1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);

        //Step-1.2 开启checkpoint并指定状态后端为fs

        //attention 新的状态后端定义法
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall-flink");
        //attention 旧的状态后端定义法
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/gmall-flink"));

        //2.Flink-CDC将读取binlog的位置信息以状态的方式保存在CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint启动程序
        //2.1 开启Checkpoint,每隔5秒钟做一次CK
        env.enableCheckpointing(5000L);
        //2.2 指定CK的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从CK自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
        //2.6 设置访问HDFS的用户名
        System.setProperty("HADOOP_USER_NAME", "holdenxiao");


       /* //每5秒做一次checkpoint
        env.enableCheckpointing(5000L);
        //Checkpoint必须在10秒内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
        //同一时间允许二个checkpoint进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        // 确认checkpoints之间的时间至少间隔3秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        */


        //Step-2 通过FlinkCDC构建SourceFunction并读取数据
        DebeziumSourceFunction<String> source = MySqlSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(DATABASE)
                //.tableList()//不加表示监控库下所有表,指定方式为db.table
                .tableList(TABLE)
                .deserializer(new StringDebeziumDeserializationSchema())//反序列化方式
                .startupOptions(StartupOptions.initial())//第一次启动时对监控的库和表做快照,意思就是先读取表中已经存在的历史数据,再跟踪binlog最新数据,类似于kafka中的earliest
                .build();
        DataStreamSource<String> streamSource = env.addSource(source);

        //Step-3 打印数据
        streamSource.print();

        //Step-4 启动任务
        env.execute();
    }
}

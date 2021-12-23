package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.gmall.realtime.app.function.DimSinkHbaseFunction;
import com.atguigu.gmall.realtime.app.function.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.common.CommonEnv;
import com.atguigu.gmall.realtime.utils.CustomerDesrialization;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import static com.atguigu.gmall.realtime.common.CommonEnv.*;
import static com.atguigu.gmall.realtime.common.CommonEnv.REAL_DATABASE;

/**
 * @ClassName gmall-flink-BaseDBApp
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日8:43 - 周二
 * @Describe 1.当你改了mysql的监控数据库, 需要重启数据库
 * @Test 数据测试流程, 1）打开ods的cdc监控gmall_flink,cdc会将变化的数据写入到ods_base_db中,打开dbapp读取gmall_realtime,
 * 等hbase中建表成功,在修改gmall_flink中数据
 * <p>
 * 数据来源:ods_base_db
 * 数据去向:hbase表和各种kafka中dwd_xx主题
 */
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        //Step-1 准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 消费Kafka中ods_base_db主题中数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(CommonEnv.ODS_DB_TOPIC, CommonEnv.DB_GROUP_ID);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //Step-3 将每行数据转换为JSON对象并过滤掉delete数据 -- 主流
        SingleOutputStreamOperator<JSONObject> filterDS = kafkaDS.map(JSONObject::parseObject)
                .filter(value -> {
                    return !"delete".equals(value.getString("type"));
                });


        //Step-4 使用FlinkCDC消费配置表并处理  --广播流,来自gmall_realtime.table_process表
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname(HOSTNAME)
                .port(3306)
                .username("root")
                .password(MYSQL_PASSWORD)
                .databaseList(REAL_DATABASE).tableList(REAL_DATABASE + ".table_process")
                .deserializer(new CustomerDesrialization())
                .startupOptions(StartupOptions.initial())//不开启会导致测试的时候广播流中没有key+type值,导致主流认为hbase中没有那个表
                .build();
        DataStreamSource<String> tableProcessDS = env.addSource(sourceFunction);

        //注册广播流,key为String类型,value为TableProcess类型,主流根据key来获取广播流数据
        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>("tableProcessState", String.class, TableProcess.class);
        //注入广播流
        BroadcastStream<String> broadcastStream = tableProcessDS.broadcast(stateDescriptor);

        //Step-5 连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = filterDS.connect(broadcastStream);


        //Step-6 分流、处理数据,主流数据根据广播流数据进行处理
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbaseTag") {
        };
        SingleOutputStreamOperator<JSONObject> kafkaJsonDS = connectedStream.process(new TableProcessFunction(hbaseTag, stateDescriptor));


        //Step-7 提取新增表中的Kafka数据和Hbase流数据
        DataStream<JSONObject> hbaseJsonDS = kafkaJsonDS.getSideOutput(hbaseTag);

        //Step-8 将kafka数据写入kafka主题,将hbase数据写入Phoenix表
        //Attention hbase就写入phoenix表,其中DimSink()是自定义Sink方法,用于想Hbase中存数据
        hbaseJsonDS.addSink(new DimSinkHbaseFunction());

        //Attention kafka写入主题,传入了一个自定义序列化方式
        kafkaJsonDS.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                /*
                 * 生产者策略:按分区轮询
                 * 丢入kafka的数据格式{"name":"我是肖华","id":18}
                 * */
                return new ProducerRecord<byte[], byte[]>(
                        jsonObject.getString("sink_table"), //流向的kafka主题
                        jsonObject.getString("after").getBytes());//将数据变成字节数组
            }
        }));

        kafkaJsonDS.print("kafka>>>>>>>>");
        hbaseJsonDS.print("hbase>>>>>>>>");

        //Step-9 启动任务
        env.execute();
    }
}

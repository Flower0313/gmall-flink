import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.CommonEnv;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * @ClassName gmall-flink-KafkaTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月15日19:49 - 周三
 * @Describe
 */
public class KafkaTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //Step-2 消费Kafka中ods_base_db主题中数据
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(CommonEnv.ODS_DB_TOPIC, CommonEnv.DB_GROUP_ID);
        DataStream<String> kafkaDS = env.addSource(kafkaSource);


        DataStream<JSONObject> map = kafkaDS.map(JSONObject::parseObject);

        map.addSink(MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject jsonObject, @Nullable Long aLong) {
                //生产者策略:按分区轮询
                return new ProducerRecord<byte[], byte[]>(
                        jsonObject.getString("table"), //流向的kafka主题
                        jsonObject.getString("after").getBytes());//将数据变成字节数组
            }
        }));


        map.print();

        env.execute();
    }
}

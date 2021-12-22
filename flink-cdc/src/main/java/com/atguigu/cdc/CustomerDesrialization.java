package com.atguigu.cdc;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Optional;

/**
 * @ClassName gmall-flink-CustomerDesrialization
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月13日10:35 - 周一
 * @Describe 自定义解析json序列化器,若不知道怎么写,可以模仿官方的序列化器
 */
public class CustomerDesrialization implements DebeziumDeserializationSchema<String> {

    private static final long serialVersionUID = 313131313139898L;

    public CustomerDesrialization() {
    }
    /*
     * 数据格式:
     * "database":"",
     * "tableName":"",
     * "type":"c u d",
     * "before":{"id":"","tm_name":""...}
     * "after":{"":"","":""...}
     * "ts":12412412
     * */

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //Step-1 创建JSON对象用于存储最终数据
        JSONObject result = new JSONObject();


        //Step-2 获取主题信息，包含数据库和表名,SourceRecord是kafka资源
        String topic = sourceRecord.topic();//库名和表名按点分割
        String[] arr = topic.split("\\.");
        String db = arr[1];
        String tableName = arr[2];


        //Step-4 获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {//防止空指针异常
            for (Field field : before.schema().fields()) {
                //将数据以 "字段:值" 的方式加入json
                beforeJson.put(field.name(), before.get(field));
            }
        }

        //Step-5 获取after数据
        Struct value2 = (Struct) sourceRecord.value();
        Struct after = value2.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            for (Field field : after.schema().fields()) {
                //将数据以 "字段:值" 的方式加入json
                afterJson.put(field.name(), after.get(field));
            }
        }

        //Step-6 获取操作类型
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();//转换成小写
        if ("create".equals(type)) {
            type = "insert";//这是为了和maxwell与canal统一,将create改为insert
        }


        //Step-7 将字段写入JSON对象
        result.put("database", db);
        result.put("table", tableName);
        result.put("type", type);
        result.put("before", beforeJson);//不要toString(),不然会变成\"这种格式
        result.put("after", afterJson);


        //Step- 输出
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}

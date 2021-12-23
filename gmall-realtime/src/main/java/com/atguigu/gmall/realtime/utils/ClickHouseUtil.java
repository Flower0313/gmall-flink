package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.bean.TransientSink;
import com.atguigu.gmall.realtime.common.GmallConfig;
import io.debezium.jdbc.JdbcConnection;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @ClassName gmall-flink-ClickHouseUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月21日13:55 - 周二
 * @Describe ClickHouse工具类
 * JdbcSink的具体用法可看:https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/jdbc/
 * <p>
 * 反射:
 * 参数:obj.getField() => field.get(obj)
 * 方法:method(args) =>  method.invoke(obj,args)
 */
public class ClickHouseUtil {
    //这里T就是写入数据的类型
    public static <T> SinkFunction<T> getJdbcSink(String sql) {
        /*
         * Attention
         *  JavaBean中的属性要和表中的字段个数相同,这样才能使用
         *  insert into 表名 (v1,v2,v3,v4) values (?, ?, ?, ?)来进行操作,
         *  但JavaBean中可能会多一些辅助字段,这些辅助字段是没必要在表中建立的
         *
         * */
        return JdbcSink.<T>sink(
                sql,
                ((ps, t) -> {
                    //1.获取你传入类中的属性(包括private),因为不同的表字段不同,不能写死
                    Field[] fields = t.getClass().getDeclaredFields();
                    //此变量用于控制映射的偏移量
                    int skipOffset = 0;
                    for (int i = 0; i < fields.length; i++) {
                        //2.取出字段名
                        Field field = fields[i];

                        //3.通过反射获得字段上的注解
                        TransientSink transientSink = field.getAnnotation(TransientSink.class);

                        //4.若字段上有注解,就执行以下逻辑
                        if (transientSink != null) {
                            System.out.println("跳过注解字段>>>" + field.getName());
                            skipOffset++;
                            continue;//直接跳过此字段,去判断下一个字段
                        }
                        //5.设置私有属性也能访问
                        field.setAccessible(true);
                        //4.根据字段名向类中取出对应的字段名
                        try {
                            //5.获取字段值
                            Object value = field.get(t);
                            /*
                             * Attention
                             * 6.将查询的值放入,这里要减去skipOffset就是因为要javaBean的属性顺序和表中字段对应上
                             *   加1是因为ps的索引是从1开始的,不懂可以去看图
                             *
                             *   或者让注解字段都放在最后,这也是一种办法
                             * */
                            ps.setObject(i + 1 - skipOffset, value);
                        } catch (IllegalAccessException e) {
                            e.printStackTrace();
                        }
                    }
                }),
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1)//1条提交一次,所以一直是1的倍数写入,若这里设置5,但只来了1条数据,是不会写入到clickhouse的
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(GmallConfig.CLICKHOUSE_DRIVER)
                        .withUrl(GmallConfig.CLICKHOUSE_URL)
                        .build()
        );
    }
}

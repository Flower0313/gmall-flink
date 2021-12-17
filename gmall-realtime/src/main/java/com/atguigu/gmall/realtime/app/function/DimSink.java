package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * @ClassName gmall-flink-DimSink
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月15日9:57 - 周三
 * @Describe
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    private Connection conn;

    //获取连接
    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(PHOENIX_DRIVER);//可有可无
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        conn = DriverManager.getConnection(PHOENIX_SERVER, properties);
        conn.setAutoCommit(true);//mysql默认是true,而phoenix默认为false
    }

    /**
     * 将数据通过sql语句写入hbase,phoenix将update和insert语句合并成了upsert
     *
     * @param value 格式{"database":"","before":"","after":"需要的字段不是全字段","type":"","table":"","sink_table":""}
     */
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement ps = null;
        JSONObject after = value.getJSONObject("after");

        Set<String> strings = after.keySet();//取出after中的key，也就是列名
        Collection<Object> values = after.values();//取出after的value，也就是列值
        //Attention 注意这里的表是hbase中的表名
        String sink_table = value.getString("sink_table");
        //拼接新增sql语句

        try {
            ps = conn.prepareStatement(String.valueOf(genUpsertSql(sink_table, strings, values)));
            int i = ps.executeUpdate();
            conn.commit();
            System.out.println(i == 1 ? "upsert成功" : "upsert失败");
        } finally {
            //若ps不为null就关闭
            Objects.requireNonNull(ps).close();
        }

    }

    /**
     * 拼接upsert的sql语句
     *
     * @param table   表名
     * @param strings 字段名
     * @param values  字段值
     * @return
     */
    private String genUpsertSql(String table, Set<String> strings, Collection<Object> values) {
        /*
        * Explain
        *  StringUtils.join(Object[] array)的作用
        * 在每个元素中间加上指定分隔符
        * */
        StringBuilder upsertSql = new StringBuilder("upsert into ")
                .append(HBASE_SCHEMA)
                .append(".").append("\"").append(table).append("\"").append("(")
                .append(StringUtils.join(strings, ","))
                .append(") ")
                .append("values('")
                .append(StringUtils.join(values, "','"))
                .append("')");

        System.out.println("建表语句:" + String.valueOf(upsertSql));
        return String.valueOf(upsertSql);
    }
}

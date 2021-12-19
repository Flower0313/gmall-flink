package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * @ClassName gmall-flink-TableProcessFunction
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日19:52 - 周二
 * @Describe 自定义TableProcessFunction方法
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject, String, JSONObject> {

    private Connection conn;
    private OutputTag<JSONObject> hbaseTag;
    private MapStateDescriptor<String, TableProcess> stateDescriptor;
    private PreparedStatement ps;


    public TableProcessFunction(OutputTag<JSONObject> hbaseTag, MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.hbaseTag = hbaseTag;
        this.stateDescriptor = stateDescriptor;
    }

    @Override//连接phoenix
    public void open(Configuration parameters) throws Exception {
        System.out.println(">>>正在连接hbase....");
        Class.forName(PHOENIX_DRIVER);
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        conn = DriverManager.getConnection(PHOENIX_SERVER, properties);
    }

    /**
     * 处理主流数据
     *
     * @param value 数据格式{"database":"","before":"","after":"","type":"","table":""}
     */
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {
        System.out.println(">>>进入主流");

        //Step-1 获取状态数据
        //获取广播过来的数据
        ReadOnlyBroadcastState<String, TableProcess> state = ctx.getBroadcastState(stateDescriptor);
        String table = value.getString("table");
        String type = value.getString("type");
        String key = table + ":" + type;
        //取出数据，广播流将数据存储在state中
        /*
         * Explain
         * Q1:为什么要用table+type作为主键呢?
         * A1:比如广播流中以A表+insert存入广播状态时,主流中的A表只有insert操作才能关联上,也就是只会
         *    读取A表中的insert数据,而update数据时不会关联上的。若你需要update的数据话,需要在表配置表中
         *    新增A表+update数据,这样广播流中就会得到了然后读入hbase
         *
         * */

        TableProcess tableProcess = state.get(key);

        /*
         * Explain 为什么这里要做null判断呢?
         * 因为有可能46张表中有一些无关的数据会变动,但是表配置表中没有数据的流动,所以广播流就没有那些变动表的数据,
         * 仅仅只是46张表中数据的变动而已,所以会导致主流多一些无关的key=table+type,而这些key在广播流中,
         * 所以这时候主流读取不到广播流中的state数据,因为广播流中根本没有相应的数据存进去。
         * */
        if (tableProcess != null) {
            //Step-2 过滤字段
            JSONObject data = value.getJSONObject("after");
            //将data的对象地址传过去,所以在filterColumn方法中修改了去除了值,也会影响data(值传递)
            filterColumn(data, tableProcess.getSinkColumns());

            //Step-3 分流
            //向value中追加sink_table信息,因为后期要区分sink到hbase还是kafka,就根据这个字段来判断
            value.put("sink_table", tableProcess.getSinkTable());

            String sinkType = tableProcess.getSinkType();
            if (TableProcess.SINK_TYPE_KAFKA.equals(sinkType)) {
                //Attention Kafka数据(事实表),写入主流
                out.collect(value);
            } else if (TableProcess.SINK_TYPE_HBASE.equals(sinkType)) {
                //Attention Hbase数据(维度表)
                System.out.println("维度数据变动:" + value);
                ctx.output(hbaseTag, value);
            }
        } else {
            System.out.println("该Key:" + key + "不存在！");
        }
    }

    /**
     * 处理广播流数据，将数据流动的情况存入广播,比如x数据从哪里流向到了哪里
     * 但只是数据在表中变动而没有流动的话是读取不到的广播流每进来一条广播数据就执行一次
     *
     * @param value 数据格式{"database":"","before":"","after":"","type":"","table":""}
     */
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        System.out.println(">>>正在将table_process表数据导入广播流");
        //Step-1 获取并解析数据
        JSONObject jsonObject = JSONObject.parseObject(value);
        //拿到after的数据
        JSONObject afterData = jsonObject.getJSONObject("after");
        String operator_type = jsonObject.getString("type");
        String table = afterData.getString("source_table");
        String type = afterData.getString("operate_type");
        //作为联合主键
        String key = table + ":" + type;

        /*
         * Attention
         *  将json转换为对应的TableProcess类;
         *  这里将json中的operate_type转换为了TableProcess类中的operateType,会自动识别对应
         * */
        TableProcess tableProcess = JSON.parseObject(afterData.toString(), TableProcess.class);

        //Step-2 在phoenix中建表
        /*
         * Q&A!!
         * Q1:若主流和广播流数据同时来,但广播流连接hbase建表很慢,在建表过程中,主流已经来了很多改动的数据,但此时广播流表都没建好,
         * 更别说将变动的数据广播出去了,那这样主流中就取不到对应的值,那主流中那些来了的数据会丢失,怎么处理?
         * A1:我的想法是,在建库时这个表的数据就需要先插入好,等全部插入好后再插入数据
         * */
        if (TableProcess.SINK_TYPE_HBASE.equals(tableProcess.getSinkType()) && !"delete".equals(operator_type)) {
            checkTable(tableProcess.getSinkTable(),
                    tableProcess.getSinkColumns(),
                    tableProcess.getSinkPk(),
                    tableProcess.getSinkExtend());
        }

        //Step-3 以键值对方式广播输出
        //BroadcastState是键值对类型
        BroadcastState<String, TableProcess> state = ctx.getBroadcastState(stateDescriptor);
        //联合主键作为key,tableProcess类作为value
        System.out.println(">>>将key:" + key + "写入广播流");
        state.put(key, tableProcess);
    }

    @Override
    public void close() throws Exception {
        conn.close();
    }

    /**
     * 拼接建表语句,若phoenix中没有表就先建表
     * 因为phoenix不像kafka,不能自自己创建表
     *
     * @param sinkTable   表名
     * @param sinkColumns 字段名
     * @param sinkPk      主键名
     * @param sinkExtend  扩展字段
     */
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) throws SQLException {
        try {
            System.out.println(">>>正在Hbase端建表");
            //Step-1 拼接建表语句的头部信息,(.append)用于向StringBuffer追加字段
            //使用create table if not exist就不需要在这里验证了
            StringBuffer createSql = new StringBuffer("create table if not exists ")
                    .append(HBASE_SCHEMA)//数据库名
                    .append(".")
                    .append(sinkTable.toUpperCase())//表名
                    .append("(");

            //Step-2 拼接建表语句的字段信息
            String[] columns = sinkColumns.split(",");
            for (int i = 0; i < columns.length; i++) {
                String column = columns[i];
                //判断该字段是否为主键,若sinkPk为null就默认id值为主键
                if ((sinkPk == null || "null".equals(sinkPk) || "".equals(sinkPk) ? "id" : sinkPk).equals(column)) {
                    createSql.append(column).append(" varchar primary key");
                } else {
                    createSql.append(column).append(" varchar");
                }
                //判断是否为最后一个字段,若不是需要加","号
                if (i < columns.length - 1) {
                    createSql.append(",");
                }
            }

            //Step-3 拼接建表语句尾部信息,注意不是所有语句都有sinkExtend
            createSql.append(")").append(sinkExtend == null || "".equals(sinkExtend) || "null".equals(sinkExtend) ? "" : sinkExtend);

            //Step-4 创建表
            ps = conn.prepareStatement(String.valueOf(createSql));
            int i = ps.executeUpdate();
            conn.commit();
            if (i == 0) {
                System.out.println(">>>表创建成功！");
            }
        } catch (Exception e) {
            throw new RuntimeException("Phoenix" + sinkTable.toUpperCase() + "建表失败");
        } finally {
            ps.close();
        }
    }

    /**
     * 我们只保留需要的字段存入hbase,其余的字段都删除,因为我们在hbase中创建表就只创建了
     * 需要的字段的列,不过滤掉多加字段会出错
     *
     * @param data        全字段
     * @param sinkColumns 需要的字段
     */
    private void filterColumn(JSONObject data, String sinkColumns) {
        String[] fields = sinkColumns.split(",");
        List<String> columns = Arrays.asList(fields);
        //将全字段中不包含变动字段的字段都删除
        data.entrySet().removeIf(next -> !columns.contains(next.getKey()));

        //复杂版写法
        /*Iterator<Map.Entry<String, Object>> iterator = data.entrySet().iterator();
        while(iterator.hasNext()){
            Map.Entry<String, Object> next = iterator.next();
            if(!columns.contains(next.getKey())){
                iterator.remove();
            }
        }*/
    }
}

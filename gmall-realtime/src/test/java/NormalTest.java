import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.TableProcess;

import static com.atguigu.gmall.realtime.common.GmallConfig.HBASE_SCHEMA;

/**
 * @ClassName gmall-flink-NormalTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日20:59 - 周二
 * @Describe
 */
public class NormalTest {
    public static void main(String[] args) {
        String str = "{\"operate_type\":\"insert\",\"sink_type\":\"hbase\",\"sink_table\":\"student\",\"source_table\":\"\",\"sink_extend\":\"engine=InnoDB default charset=utf8\",\"sink_pk\":\"null\",\"sink_columns\":\"id,name,age\"}";

        TableProcess process = JSON.parseObject(str, TableProcess.class);

        System.out.println(process);
        checkTable(process.getSinkTable(), process.getSinkColumns(), process.getSinkPk(),
                process.getSinkExtend());


    }

    private static void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        //Step-1 拼接建表语句的头部信息,(.append)用于向StringBuffer追加字段
        //使用create table if not exist就不需要在这里验证了
        StringBuffer createSql = new StringBuffer("create table if not exist ")
                .append("default")//数据库名
                .append(".")
                .append(sinkTable)//表名
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
        createSql.append(")").append(sinkExtend == null ? "" : sinkExtend);

        System.out.println(createSql);

    }
}

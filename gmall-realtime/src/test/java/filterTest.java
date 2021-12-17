import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.TableProcess;

/**
 * @ClassName gmall-flink-filterTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月15日0:46 - 周三
 * @Describe
 */
public class filterTest {
    public static void main(String[] args) {
        String sql = "{\"operate_type\":\"insert\",\"sink_type\":\"hbase\",\"sink_table\":\"student\",\"source_table\":\"\",\"sink_extend\":\"engine=InnoDB default charset=utf8\",\"sink_pk\":\"id\",\"sink_columns\":\"id,name,age\"}";

        TableProcess tableProcess = JSON.parseObject(sql, TableProcess.class);
        System.out.println(tableProcess);

    }
}

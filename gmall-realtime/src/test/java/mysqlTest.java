import com.atguigu.gmall.realtime.utils.MyJdbcUtil;

import java.sql.*;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * @ClassName gmall-flink-mysqlTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月20日8:31 - 周一
 * @Describe
 */
public class mysqlTest {
    public static void main(String[] args) throws Exception {
        System.out.println(MyJdbcUtil.isRealExists("base_dic", "isert"));
        MyJdbcUtil.close();
    }
}

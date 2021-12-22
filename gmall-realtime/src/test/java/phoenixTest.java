import com.alibaba.fastjson.JSONObject;

import java.sql.*;
import java.util.ArrayList;
import java.util.Properties;

import static com.atguigu.gmall.realtime.common.GmallConfig.PHOENIX_DRIVER;
import static com.atguigu.gmall.realtime.common.GmallConfig.PHOENIX_SERVER;

/**
 * @ClassName gmall-flink-phoenixTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日22:10 - 周二
 * @Describe
 */
public class phoenixTest {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        Connection conn = DriverManager.getConnection(PHOENIX_SERVER, properties);

        PreparedStatement ps = conn.prepareStatement("select * from GMALL0726_REALTIME.DIM_SKU_INFO where id='12'");

        ResultSet resultSet = ps.executeQuery();
        //Attention getMetaData是获得表结构
        ResultSetMetaData metaData = resultSet.getMetaData();
        /*
         * resultSet.getMetaData().getColumnCount();//取得指定数据表的字段总数，返回值为Int型
         * resultSet.getMetaData().getColumnName(n);//取得第n个字段的名称，返回值为String型
         * resultSet.getMetaData().getColumnLabel(n);//返回n所对应的列的显示标题
         * resultSet.getMetaData().getColumnDisplaySize(n);//缺的第n个字段的长度，返回值为Int型
         * resultSet.getMetaData().getColumnTypeName(n);//返回第n个字段的数据类型
         * resultSet.getMetaData().isReadOnly(n);//返回该n所对应的列是否只读.
         * resultSet.getMetaData().isNullable(n)返回该n所对应的列是否可以为空.
         * resultSet.getMetaData().getSchemaName(n)n列的模式
         * resultSet.getMetaData().getPrecision(n);取得第n列字段类型长度的精确度
         * resultSet.getMetaDta().getScale(n);第n列小数点后的位数
         * resultSet.getMetaData().isAutoIncrement(n);第n列是否为自动递增
         * resultSet.getMetaData().isCurrency(n);是否为货币类型
         * resultSet.getMetaData().isSearchable(n);n列能否出现在where语句中.
         * */

        System.out.println("字段的个数:" + metaData.getColumnCount());
        ArrayList<JSONObject> jsonObjects = new ArrayList<>();

        //next会将光标往下移动,然后返回bool,true为有值,false为没值
        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();

            //遍历每个字段,让取出字段对应的值
            for (int i = 1; i < metaData.getColumnCount() + 1; i++) {

                //System.out.println(metaData.getColumnName(i) + ":" + resultSet.getObject(i));
                jsonObject.put(metaData.getColumnName(i), resultSet.getObject(i));
                jsonObjects.add(jsonObject);
            }
        }
        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }

        ps.close();
        conn.close();
    }
}

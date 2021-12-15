package com.atguigu.gmall.realtime.app.dwd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import static com.atguigu.gmall.realtime.common.GmallConfig.PHOENIX_DRIVER;
import static com.atguigu.gmall.realtime.common.GmallConfig.PHOENIX_SERVER;

/**
 * @ClassName gmall-flink-test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日22:18 - 周二
 * @Describe
 */
public class test {
    public static void main(String[] args) throws Exception {
        System.out.println("开始执行");
        //Class.forName(PHOENIX_DRIVER);
        Properties properties = new Properties();
        properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
        Connection conn = DriverManager.getConnection(PHOENIX_SERVER, properties);
        //String selectSql = "select * from \"holden\".\"user\"";

        //String createSql = "create table if not exists \"holden\".\"user\"(id varchar primary key,name varchar,age varchar)";

        //幂等性
        String upsertSql = "upsert into \"holden\".\"user\"(id,name,age) values('1010','kkk','9')";
        System.out.println(upsertSql);

        PreparedStatement ps = conn.prepareStatement(upsertSql);

        //查询
        /*
        conn.prepareStatement()
        ResultSet resultSet = ps.executeQuery();

        while (resultSet.next()) {
            System.out.println(resultSet.getString(1) + ":" +
                    resultSet.getString(2) + ":" +
                    resultSet.getString(3));
        }*/

        int i = ps.executeUpdate();
        conn.commit();
        System.out.println("i:" + i);
        if (0 == i) {
            System.out.println("操作成功！");
        }
        ps.close();
        conn.close();
    }
}

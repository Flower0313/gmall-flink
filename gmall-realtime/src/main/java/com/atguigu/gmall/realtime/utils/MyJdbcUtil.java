package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;
import org.codehaus.jackson.map.util.BeanUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName gmall-flink-MyJdbcUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:39 - 周六
 * @Describe
 */
public class MyJdbcUtil {
    /**
     * @param conn              数据库连接
     * @param sql               sql语句
     * @param clz               T的类型
     * @param underScoreToCamel 是否需要转换驼峰命名
     * @param <T>               什么泛型方法
     * @return
     */
    public static <T> List<T> queryList(Connection conn, String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        ArrayList<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行查询
        ResultSet resultSet = ps.executeQuery();
        //解析
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();//字段的个数

        while (resultSet.next()) {
            //创建泛型对象,弱类型,只能调用构造方法
            T t = clz.newInstance();

            //这里从1开始,因为jdbc的索引就是从1开始
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换驼峰命名
                if (underScoreToCamel) {
                    //将下划线命名转换为驼峰命名
                    columnName = CaseFormat.LOWER_UNDERSCORE
                            .to(CaseFormat.LOWER_CAMEL, columnName);
                }
                //获取列值
                Object object = resultSet.getObject(i);

                //给泛型对象赋值,参数一是bean对象,参数二是列名,参数三是列值
                BeanUtils.setProperty(t, columnName, object);//不能使用copyProperty

            }
            resultList.add(t);
        }
        ps.close();
        resultSet.close();


        //返回结果集合
        return resultList;
    }

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/gmall_flink?characterEncoding=utf-8&useSSL=false",
                "root", "root");
        List<JSONObject> jsonObjects = queryList(conn, "select * from base_trademark", JSONObject.class, false);

        for (JSONObject jsonObject : jsonObjects) {
            System.out.println(jsonObject);
        }
        //关闭连接
        conn.close();

    }
}

package com.atguigu.gmall.realtime.utils;

import com.atguigu.gmall.realtime.common.GmallConfig;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.atguigu.gmall.realtime.common.GmallConfig.PHOENIX_SERVER;

/**
 * @ClassName gmall-flink-PhoenixUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:21 - 周六
 * @Describe 将phoenix中的查询出的数据转换为JSONObject格式
 */
public class PhoenixUtil {
    public static Connection conn;

    private static Connection init() {
        try {
            //phoenix连接
            Properties properties = new Properties();
            properties.put("phoenix.schema.isNamespaceMappingEnabled", "true");
            conn = DriverManager.getConnection(PHOENIX_SERVER, properties);
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取连接失败！");
        }
    }


    /**
     * @param conn              数据库连接
     * @param sql               sql语句
     * @param clz               T的类型
     * @param underScoreToCamel 是否需要转换驼峰命名
     * @param <T>               什么泛型方法
     * @return 将查询的数据以JSON的格式返回
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        if (conn == null) {
            conn = init();
        }
        ArrayList<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet resultSet = null;
        try {
            //预编译SQL
            ps = conn.prepareStatement(sql);
            //执行查询
            resultSet = ps.executeQuery();
            //解析
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();//字段的个数

            //遍历每行数据,功能:将每行数据转换为Json格式
            while (resultSet.next()) {
                //创建泛型对象,弱类型,只能调用构造方法,这里我们传的是JSONObject,那么会就相当于调用其构造方法
                T t = clz.newInstance();

                //这里从1开始,因为jdbc的索引就是从1开始,遍历所有字段
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

                    /*
                     * Attention
                     * 给泛型对象赋值,参数一是bean对象,参数二是列名,参数三是列值
                     * 其实这里是在调用JSONObject中的get和set方法,然后给JSONObject对象赋值
                     * */
                    BeanUtils.setProperty(t, columnName, object);//不能使用copyProperty

                }
                //System.out.println("改进后t:" + t);
                resultList.add(t);//将读到的每行数据转换为JSONObject存入List集合
            }
        } catch (Exception e) {
            throw new RuntimeException(">>>查询维度信息失败!");
        } finally {
            if (ps != null) {
                ps.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }

        ps.close();
        resultSet.close();

        //返回结果集合
        return resultList;
    }
}

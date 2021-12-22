package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import static com.atguigu.gmall.realtime.common.GmallConfig.*;

/**
 * @ClassName gmall-flink-MyJdbcUtil
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:39 - 周六
 * @Describe 万能连接jdbc类, 将查询的数据转换成为你指定的类型输出
 */
public class MyJdbcUtil {
    private static Connection conn;

    private static Connection init() {
        try {
            //mysql连接
            Class.forName(MYSQL_DRIVER);
            conn = DriverManager.getConnection(MYSQL_SERVER, "root", "root");
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("获取Mysql连接失败！");
        }
    }


    /**
     * @param sql               sql语句
     * @param clz               T的类型
     * @param underScoreToCamel 是否需要转换驼峰命名
     * @param <T>               什么泛型方法
     * @return
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underScoreToCamel) throws Exception {
        if (conn == null) {
            conn = init();
        }

        ArrayList<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement ps = conn.prepareStatement(sql);
        //执行查询
        ResultSet resultSet = ps.executeQuery();
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
                 * 其实在调用JSONObject中的get和set方法
                 * */
                BeanUtils.setProperty(t, columnName, object);//不能使用copyProperty

            }
            //System.out.println("改进后t:" + t);
            resultList.add(t);//将读到的每行数据转换为JSONObject存入List集合
        }
        ps.close();
        resultSet.close();

        //返回结果集合
        return resultList;
    }

    public static void close() throws Exception {
        try {
            conn.close();
        } catch (Exception e) {
            conn.close();
            throw new RuntimeException("mysql关闭出错");
        }
    }


    /**
     * 此方法用于主流数据比广播流数据来的快导致广播流的key还没传递过去,主流误认为没有此表,最终导致数据丢失
     *
     * @param source_table 来源表
     * @param operate_type 操作类型
     * @return 返回一个TableProcess对象, 但可能是null
     * @throws Exception
     */
    public static TableProcess isRealExists(String source_table, String operate_type) throws Exception {
        if (conn == null) {
            conn = init();
        }
        String sql = "select * from gmall_realtime.table_process where source_table='" + source_table + "' and operate_type='" + operate_type + "'";
        PreparedStatement ps = conn.prepareStatement(sql);
        ResultSet resultSet = ps.executeQuery();
        TableProcess tableProcess = null;
        if (resultSet.next()) {
            List<JSONObject> jsonObjects = queryList(sql, JSONObject.class, false);
            if (jsonObjects.size() > 0) {
                tableProcess = JSON.parseObject(jsonObjects.get(0).toJSONString(), TableProcess.class);
            }
        }
        resultSet.close();
        ps.close();
        //没有的话直接返回null
        return tableProcess;
    }

    public static void main(String[] args) throws Exception {


    }
}

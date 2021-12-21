package com.atguigu.gmall.realtime.bean;

import lombok.Data;

/**
 * @ClassName gmall-flink-TableProcess
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月14日14:03 - 周二
 * @Describe ODS层表配置类 -- 对应gmall_realtime.table_process
 */

@Data
public class TableProcess {
    //动态分流Sink常量
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    /*
     * 联合主键:sourceTable + operateType
     * */

    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表或主题
    String sinkTable;
    //输出字段,这些字段会存入hbase中
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;


}

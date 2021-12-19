package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;

/**
 * @ClassName gmall-flink-DimJoinFunction
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月19日14:10 - 周日
 * @Describe 维度关联接口
 */
public interface DimJoinFunction<T> {
    //获取数据中的所要关联维度的主键
    String getKey(T input);

    //关联事实数据和维度数据
    void join(T input, JSONObject dimInfo) throws Exception;
}

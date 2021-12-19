package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName gmall-flink-DimAsyncFunction
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月19日11:13 - 周日
 * @Describe
 */
public abstract class DimAsyncFunction<IN, OUT> extends RichAsyncFunction<IN, OUT> implements DimJoinFunction<IN> {
    private ThreadPoolExecutor threadPoolExecutor;
    private String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池
        threadPoolExecutor = ThreadPoolUtil.getInstance();
    }

    @Override//线程开始执行的方法
    public void asyncInvoke(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        threadPoolExecutor.submit(new Runnable() {
            @SneakyThrows//接收异常的注解
            @Override
            public void run() {
                //1.获取查询的主键值,注意getKey是重写方法
                String id = getKey(input);

                //2.关联到事实数据上
                JSONObject dimInfo = DimUtil.getDimInfo(tableName, id);

                if (dimInfo != null && dimInfo.size() > 0) {
                    join(input, dimInfo);
                }
                //3.继续向下游传输
                resultFuture.complete(Collections.singletonList((OUT) input));
            }
        });
    }

    @Override
    public void timeout(IN input, ResultFuture<OUT> resultFuture) throws Exception {
        /*
         * Explain 异步请求发出多久后未得到响应即被认定失败,它可以防止一直等待不到响应的请求
         * 当异步I/O请求超时的时候,默认会抛出异常并重启作业,
         * 如果你想处理超时事件时,可以重写AsyncFunction#timeout方法。
         * */
        System.out.println("TimeOut" + input);
    }
}
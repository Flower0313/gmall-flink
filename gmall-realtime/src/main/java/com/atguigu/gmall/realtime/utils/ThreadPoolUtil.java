package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @ClassName gmall-flink-ThreadPoolExecutor
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日23:23 - 周六
 * @Describe 线程池对象的单例方法
 */
public class ThreadPoolUtil {
    //声明线程池
    public static ThreadPoolExecutor pool;

    //私有化构造器
    private ThreadPoolUtil() {

    }

    //单例对象,线程安全
    public static ThreadPoolExecutor getInstance() {
        /*
         * Explain:
         *  corePoolSize:指定了线程池中的核心线程数,当任务过来时就会先创建这么多核心线程数
         *  maximumPoolSize:指定了线程池中的最大线程数量,这个参数会根据你使用的workQueue任务队列的类型,决定线程池会开辟的最大线程数量;
         *  keepAliveTime:非核心线程超过规定时间就会被回收掉,非核心线程也就是maxPoolSize-corePoolSize
         *  unit:keepAliveTime的单位
         *  workQueue:任务队列,当池子中的工作线程数大于corePoolSize时,这时新进来的任务会被放到队列中,当队列满了才会创建非核心线程
         *  LinkedBlockingDeque:队列长度不受限制,当请求越来越多时(任务处理速度跟不上任务提交速度)可能导致内存占用过多或OOM
         * */
        if (pool == null) {
            //Attention 静态方法的同步监视器就是类本身
            synchronized (ThreadPoolUtil.class) {
                if (pool == null) {
                    pool = new ThreadPoolExecutor(
                            4,//核心线程数为4,核心线程是一直存在的
                            20,//最大线程可以添加到20,也就是非核心线程数是16
                            300L,//当非核心线程数(16个)超过300秒没有被使用就释放掉
                            TimeUnit.SECONDS,//单位秒
                            new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));//workQueue,队列满了才会去创建第5个非核心线程
                }
            }
        }
        return pool;
    }
}

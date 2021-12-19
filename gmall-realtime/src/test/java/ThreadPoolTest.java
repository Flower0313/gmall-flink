import java.util.concurrent.*;

/**
 * @ClassName gmall-flink-ThreadTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月19日12:17 - 周日
 * @Describe 线程池的用法
 * 1.提供响应速度(减少了创建新线程的时间) 2.降低资源消耗(重复利用线程池中的线程,不需要每次都创建) 3.便于线程管理
 * corePoolSize:核心池的大小
 * maximumPoolSize:最大线程数
 * keepAliveTime:线程没有任务时最多保持多长时间后会终止
 */
public class ThreadPoolTest {
    public static void main(String[] args) {
        //1.提供指定线程数量的线程池
        //ExecutorService service = Executors.newFixedThreadPool(10);
        //ThreadPoolExecutor service1 = (ThreadPoolExecutor) service;

        ThreadPoolExecutor pool = new ThreadPoolExecutor(3, 20, 300L, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));

        //设置线程池的属性
        pool.setCorePoolSize(10);
        pool.setMaximumPoolSize(50);
        pool.setKeepAliveTime(100, TimeUnit.SECONDS);


        //2.执行指定的线程操作,需要提供实现Runnable接口或Callable接口实现类的对象
        //service.execute(new NumberThread1());
        pool.submit(new NumberThread1());//异步
        //pool.submit(new NumberThread1());//使用submit也可以,只不过submit还可以接收Callable接口
        System.out.println("hello");
        //service.execute(new NumberThread2());

        pool.execute(new NumberThread2());//异步

        //3.关闭连接池
        pool.shutdown();
        //service.shutdown();//关闭连接池
    }
}

class NumberThread1 implements Runnable {
    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            if (i % 2 == 0) {
                System.out.println(Thread.currentThread().getName() + "偶数:" + i);
            }
        }
    }
}

class NumberThread2 implements Runnable {
    @Override
    public void run() {
        for (int i = 0; i < 100; i++) {
            if (i % 2 != 0) {
                System.out.println(Thread.currentThread().getName() + "奇数:" + i);
            }
        }
    }
}

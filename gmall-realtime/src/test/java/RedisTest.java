import com.atguigu.gmall.realtime.bean.RedisUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;

import static com.atguigu.gmall.realtime.bean.RedisUtil.jedisPool;

/**
 * @ClassName gmall-flink-RedisTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日14:07 - 周六
 * @Describe
 */
public class RedisTest {
    public static void main(String[] args) {
        String name ="flower";
        System.out.println(name.toUpperCase());
    }
}

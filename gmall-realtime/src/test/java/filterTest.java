import com.alibaba.fastjson.JSON;
import com.atguigu.gmall.realtime.bean.TableProcess;

import java.math.BigDecimal;

/**
 * @ClassName gmall-flink-filterTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月15日0:46 - 周三
 * @Describe
 */
public class filterTest {
    public static void main(String[] args) {
        BigDecimal a = BigDecimal.valueOf(3);
        BigDecimal b = BigDecimal.valueOf(5);
        a.add(b);
        System.out.println(a);

    }
}

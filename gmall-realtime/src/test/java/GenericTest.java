import java.util.List;

/**
 * @ClassName gmall-flink-GenericTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:29 - 周六
 * @Describe
 */
public class GenericTest {
    public static void main(String[] args) {
        GenUtils.<String>show("s");
    }
}

class GenUtils {
    public static <T> List<T> show(T sql) {
        System.out.println(sql.getClass());
        return null;
    }
}
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * @ClassName gmall-flink-HashTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月22日1:17 - 周三
 * @Describe
 */
public class HashTest {
    public static void main(String[] args) {
        Set sets = new HashSet();
        Set<Integer> singleton = Collections.singleton(313);

        sets.add(singleton);
        sets.add(Collections.singleton(515));
        sets.add(818);
        System.out.println(new Date().getTime());

        sets.addAll(Collections.singleton(2));
        System.out.println(sets);

    }
}

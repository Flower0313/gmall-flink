import com.atguigu.gmall.realtime.bean.Student;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

/**
 * @ClassName gmall-flink-BeanUtilsTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日9:07 - 周六
 * @Describe
 */
public class BeanUtilsTest {
    public static void main(String[] args) throws Exception{
        HashMap<String, String> map = new HashMap<>();
        BeanUtils.setProperty(map, "name", "flower");//它会反射寻找get\set方法为我们进行赋值
        //System.out.println(map);

        /*
        * Conclusion
        * BeanUtils.setProperty使用的前提就是操作的Bean类必须要有get和set方法
        * sex每天get、set方法,就失效了
        * */
        Student stu = new Student();
        BeanUtils.setProperty(stu,"name","xiao");
        BeanUtils.setProperty(stu,"age","13");
        BeanUtils.setProperty(stu,"sex","男");
        System.out.println(stu);

    }
}

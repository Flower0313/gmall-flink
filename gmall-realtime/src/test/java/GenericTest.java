
import org.apache.commons.beanutils.BeanUtils;
import com.atguigu.gmall.realtime.bean.Student;

import java.lang.reflect.InvocationTargetException;

/**
 * @ClassName gmall-flink-GenericTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:29 - 周六
 * @Describe
 */
public class GenericTest {
    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException {
        Student xiao = new Student("xiao", 22);
        Teacher teacher = new Teacher();
        BeanUtils.copyProperties(xiao,teacher );
        System.out.println(teacher + "||" + xiao);
    }
}




class Teacher {
    String name;

    public Teacher() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Teacher{" +
                "name='" + name + '\'' +
                '}';
    }
}
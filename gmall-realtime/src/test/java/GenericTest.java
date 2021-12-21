
import com.atguigu.gmall.realtime.bean.TransientSink;
import org.apache.commons.beanutils.BeanUtils;
import com.atguigu.gmall.realtime.bean.Student;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

/**
 * @ClassName gmall-flink-GenericTest
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日0:29 - 周六
 * @Describe
 */
public class GenericTest {
    public static void main(String[] args) throws InvocationTargetException, IllegalAccessException, NoSuchFieldException {
        Teacher teacher = new Teacher();
        teacher.setName("flower");
        teacher.age = "13";
        Field[] fields = teacher.getClass().getDeclaredFields();
        for (int i = 0; i < fields.length; i++) {
            Field field = fields[i];
            TransientSink annotation = field.getAnnotation(TransientSink.class);

            if (annotation != null) {
                System.out.println("跳过字段:" + field.getName());
            }
            Object o = field.get(teacher);//根据字段名取出字段值
            System.out.println(o);
        }

    }
}


class Teacher {
    String name;

    @TransientSink
    public String age;

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

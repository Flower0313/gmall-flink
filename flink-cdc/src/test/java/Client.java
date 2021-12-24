/**
 * @ClassName gmall-flink-Client
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月24日10:57 - 周五
 * @Describe
 */
public class Client {
    static Person person = new Student();
    public static void main(String[] args) {
        person.getGM();
    }
}

interface Person{
    String getGM();
}

class Student implements Person{

    @Override
    public String getGM() {
        return "niubi";
    }
}
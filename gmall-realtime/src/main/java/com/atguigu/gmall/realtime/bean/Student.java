package com.atguigu.gmall.realtime.bean;

/**
 * @ClassName gmall-flink-Student
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月18日9:06 - 周六
 * @Describe
 */
public class Student {
    private String name;
    private Integer age;
    public String sex;

    public Student() {
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }

    public Student(String name, Integer age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}

package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;


/**
 * @ClassName gmall-flink-TransientSink
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月21日16:08 - 周二
 * @Describe 这是自定义注解, 向ClickHouse中写入数据时, 若有写字段不需要序列化传输, 就可以中这个注解标记
 * <p>
 * <p>
 * RetentionPolicy.SOURCE：只在本编译单元的编译过程中保留，并不写入Class文件中。这种注解主要用于在本编译单元
 * (这里一个Java源码文件可以看作一个编译单元)内触发注解处理器(annotation processor)的相关处理，例如说可以让注
 * 解处理器相应地生成一些代码，或者是让注解处理器做一些额外的类型检查。
 * <p>
 * <p>
 * RetentionPolicy.CLASS：在编译的过程中保留并且会写入Class文件中,但是JVM在加载类的时候不需要将其加载为运行时
 * 可见的（反射可见）的注解。这里很重要的一点是编译多个Java文件时的情况：假如要编译A.java源码文件和B.class文件，
 * 其中A类依赖B类,并且B类上有些注解希望让A.java编译时能看到，那么B.class里就必须要持有这些注解信息才行。同时
 * 我们可能不需要让它在运行时对反射可见（例如说为了减少运行时元数据的大小之类），所以会选择CLASS而不是RUNTIME。
 * <p>
 * <p>
 * RetentionPolicy.RUNTIME:在编译过程中保留,会写入Class文件,并且JVM加载类的时候也会将其加载为反射可见的注解
 * 。这就不用多说了,例如说Spring的依赖注入就会在运行时通过扫描类上的注解来决定注入啥。
 */

@Target(FIELD)
@Retention(RUNTIME)//定义注解在JVM运行时保留
public @interface TransientSink {
}

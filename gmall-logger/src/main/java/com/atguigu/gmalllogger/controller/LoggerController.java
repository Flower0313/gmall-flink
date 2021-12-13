package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ClassName gmall-flink-LoggerController
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月11日22:49 - 周六
 * @Describe com.atguigu.gmalllogger.controller.LoggerController
 */

@RestController//=@Controller+@ResponseBody,表示返回普通对象而不是页面
@Slf4j
public class LoggerController {

    @Autowired//成员属性字段
    private KafkaTemplate<String, String> kafkaTemplate;


    @RequestMapping("test1")
    public String test1() {
        System.out.println("11111");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String name,
                        @RequestParam("age") int age
    ) {
        System.out.println(name + ":" + age);
        return name + ":" + age;
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {
        //落盘info级别数据,也有error和warn级别的
        log.info(jsonStr);//logback.xml也声明了日志打印的级别
        //log.error(jsonStr);
        //log.warn(jsonStr);
        //log.debug(jsonStr);

        //Attention 写入kafka,若你没有对应的主题,这行代码是不会自动创建的
        kafkaTemplate.send("ods_base_log", jsonStr);

        System.out.println(jsonStr);
        return "success";
    }
}

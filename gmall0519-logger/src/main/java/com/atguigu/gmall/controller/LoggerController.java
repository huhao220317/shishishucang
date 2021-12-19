package com.atguigu.gmall.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: Felix
 * Date: 2021/10/30
 * Desc: 日志采集服务
 */
@RestController
@Slf4j
public class LoggerController {
    //private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LoggerController.class);

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @RequestMapping("/applog")
    public String logger(@RequestParam("param") String logStr) {
        //打印输出  System.out.println(logStr);
        //借助logback组件       打印输出 + 落盘
        log.info(logStr);
        //将日志写到kafka主题中
        kafkaTemplate.send("ods_base_log", logStr);
        return "success";
    }
}

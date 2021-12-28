package com.bjke.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Slf4j
public class LoggerController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping("test")
    public String test1() {
        System.out.println("success");
        return "success";
    }

    @RequestMapping("test2")
    public String test2(@RequestParam("name") String nn) {
        return nn;
    }

    @RequestMapping("applog")
    public String getLog(@RequestParam("param") String jsonStr) {
        System.out.println(jsonStr);
        // 数据落盘
        log.info(jsonStr);
        // 数据写入kafka
        kafkaTemplate.send("ods_base_log", jsonStr);

        return "success";
    }
}

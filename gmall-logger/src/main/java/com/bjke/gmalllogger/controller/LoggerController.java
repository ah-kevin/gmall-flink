package com.bjke.gmalllogger.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class LoggerController {

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

        // 数据写入kafka
        return "success";
    }
}

package com.atguigu.gmall.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Author: Felix
 * Date: 2021/10/30
 * Desc: SpringMVC处理请求内容回顾
 *
 * @Controller 将对象的创建交给Spring容器, 如果当前类的方法返回的是字符串的话，那么会当做页面的跳转进行处理
 * @ResponseBody 告诉SpringMVC，当前方法返回字符串不是进行页面跳转
 * @RestController =     @Controller + @ResponseBody
 * @RequestMapping 拦截请求，将请求交给对应的方法进行处理
 * @RequestParam 不是必须的，接收请求中的参数
 */
@RestController
public class FirstController {
    @RequestMapping("/first")
    public String first(@RequestParam("heihei") String username, @RequestParam(value = "haha", defaultValue = "atguigu") String password) {
        System.out.println(username + ":::" + password);
        return "success";
    }
}

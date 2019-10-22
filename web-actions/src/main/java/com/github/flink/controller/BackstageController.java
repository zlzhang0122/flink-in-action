package com.github.flink.controller;

import com.github.flink.client.RedisClient;
import com.github.flink.domain.ContactEntity;
import com.github.flink.service.ContactService;
import com.github.flink.utils.Result;
import com.github.flink.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/21 7:18 PM
 */
@Controller
public class BackstageController {

    @Resource
    private RedisClient redisClient;

    @Autowired
    private ContactService contactService;

    private int TOP_SIZE = 10;

    @GetMapping("/index")
    public String getBackStage(Model model){
        List<String> topList = redisClient.getTopList(TOP_SIZE);

        List<ContactEntity> topProduct = contactService.selectByIds(topList);
        model.addAttribute("topProduct", topProduct);

        return "index";
    }

    @ResponseBody
    @GetMapping("/meter")
    public Result getMeter(){
        String meter = redisClient.getMeter();
//        String meter = "69";

        return ResultUtil.success(meter);
    }
}

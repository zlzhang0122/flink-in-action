package com.github.flink.controller;

import com.github.flink.dto.ProductDto;
import com.github.flink.service.KafkaService;
import com.github.flink.service.RecommandService;
import com.github.flink.utils.Result;
import com.github.flink.utils.ResultUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.IOException;
import java.util.List;

/**
 * 推荐页
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:43 PM
 */
@Controller
public class RecommandController {
    @Autowired
    private RecommandService recommandService;

    @Autowired
    private KafkaService kafkaService;

    @GetMapping("/recommand")
    public String recommandByUserId(@RequestParam("userId") String userId,
                                    Model model) throws IOException{
        List<ProductDto> hotList = recommandService.recommandByHotList();
        List<ProductDto> itemCfCoeffList = recommandService.recommandByItemCfCoeff();
        List<ProductDto> productCoeffList = recommandService.recommandByProductCoeff();

        model.addAttribute("userId", userId);
        model.addAttribute("hotList", hotList);
        model.addAttribute("itemCfCoeffList", itemCfCoeffList);
        model.addAttribute("productCoeffList", productCoeffList);

        return "user";
    }

    @GetMapping("/log")
    @ResponseBody
    public Result logToKafka(@RequestParam("id") String userId,
                             @RequestParam("prod") String productId,
                             @RequestParam("action") String action){
        String log = kafkaService.makeLog(userId, productId, action);
        kafkaService.send(null, log);
        return ResultUtil.success();
    }
}

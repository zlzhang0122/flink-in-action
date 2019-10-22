package com.github.flink.service.impl;

import com.github.flink.client.RedisClient;
import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.dto.ProductDto;
import com.github.flink.service.ContactService;
import com.github.flink.service.ProductService;
import com.github.flink.service.RecommandService;
import com.github.flink.service.UserScoreService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:53 PM
 */
@Service("recommandService")
public class RecommandServiceImpl implements RecommandService {
    private final Logger logger = LoggerFactory.getLogger(RecommandService.class);

    @Autowired
    private UserScoreService userScoreService;

    @Autowired
    private ProductService productService;

    @Autowired
    private ContactService contactService;

    @Resource
    private RedisClient redisClient;

    private final static int TOP_SIZE = 10;

    private final static int PRODUCT_LIMIT = 3;

    @Override
    public List<ProductScoreEntity> userRecommand(String userId) throws IOException {
        return null;
    }

    @Override
    public List<ProductDto> recommandByHotList() {
        return null;
    }

    @Override
    public List<ProductDto> recommandByItemCfCoeff() throws IOException {
        return null;
    }

    @Override
    public List<ProductDto> recommandByProductCoeff() throws IOException {
        return null;
    }
}

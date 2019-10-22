package com.github.flink.service.impl;

import com.github.flink.client.RedisClient;
import com.github.flink.domain.ContactEntity;
import com.github.flink.domain.ProductEntity;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

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
        List<String> topList = getDefaultTop();
        int topSize = topList.size();

        List<ContactEntity> contactEntityList = contactService.selectByIds(topList);
        List<ProductEntity> productEntityList = productService.selectByIds(topList);
        return fillProductDto(topList, contactEntityList, productEntityList, topSize);
    }

    @Override
    public List<ProductDto> recommandByItemCfCoeff() throws IOException {
        return null;
    }

    @Override
    public List<ProductDto> recommandByProductCoeff() throws IOException {
        return null;
    }

    private List<ProductDto> fillProductDto(List<String> list, List<ContactEntity> contactEntityList,
                                            List<ProductEntity> productEntityList, int topSize){
        List<ProductDto> productDtoList = new ArrayList<>();
        for(int i = 0; i < topSize; i++){
            String topId = list.get(i);
            ProductDto productDto = new ProductDto();
            productDto.setScore(TOP_SIZE + 1 - i);
            for(int j = 0; j < topSize; j++){
                if(topId.equalsIgnoreCase(String.valueOf(contactEntityList.get(j).getId()))){
                    productDto.setContactEntity(contactEntityList.get(j));
                }
                if(topId.equalsIgnoreCase(String.valueOf(productEntityList.get(j).getProductId()))){
                    productDto.setProductEntity(productEntityList.get(j));
                }
            }

            productDtoList.add(productDto);
        }

        return productDtoList;
    }

    private List<String> getDefaultTop(){
        List<String> topList = redisClient.getTopList(TOP_SIZE);
        topList = topList.stream().filter(Objects::nonNull).collect(Collectors.toList());
        if(topList.size() < 10){
            topList.addAll(productService.selectInitPro(100));
            topList = topList.stream().distinct().collect(Collectors.toList());

            logger.info("top: {}", topList);
        }

        return topList;
    }
}

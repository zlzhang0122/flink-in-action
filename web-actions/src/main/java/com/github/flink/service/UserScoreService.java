package com.github.flink.service;

import com.github.flink.domain.ProductEntity;
import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.domain.UserScoreEntity;

import java.io.IOException;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:48 PM
 */
public interface UserScoreService {
    UserScoreEntity calUserScore(String userId) throws IOException;

    List<ProductScoreEntity> getProductScore(UserScoreEntity userScoreEntity);

    List<ProductScoreEntity> getTopRankProduct(String userId) throws IOException;

    List<ProductEntity> getTopProductFrom(List<String> products);
}

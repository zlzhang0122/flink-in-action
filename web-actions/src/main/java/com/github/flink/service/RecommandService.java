package com.github.flink.service;

import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.dto.ProductDto;

import java.io.IOException;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:44 PM
 */
public interface RecommandService {
    List<ProductScoreEntity> userRecommand(String userId) throws IOException;

    List<ProductDto> recommandByHotList();

    List<ProductDto> recommandByItemCfCoeff() throws IOException;

    List<ProductDto> recommandByProductCoeff() throws IOException;
}

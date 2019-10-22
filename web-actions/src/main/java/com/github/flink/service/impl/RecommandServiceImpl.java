package com.github.flink.service.impl;

import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.dto.ProductDto;
import com.github.flink.service.RecommandService;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 7:53 PM
 */
@Service("recommandService")
public class RecommandServiceImpl implements RecommandService {
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

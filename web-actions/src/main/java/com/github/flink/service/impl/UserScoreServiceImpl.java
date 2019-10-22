package com.github.flink.service.impl;

import com.github.flink.client.RedisClient;
import com.github.flink.domain.ProductEntity;
import com.github.flink.domain.ProductScoreEntity;
import com.github.flink.domain.UserScoreEntity;
import com.github.flink.service.ProductService;
import com.github.flink.service.UserScoreService;
import com.github.flink.utils.client.HbaseClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:59 PM
 */
@Service("userScoreService")
public class UserScoreServiceImpl implements UserScoreService {

    private RedisClient redisClient = new RedisClient();

    @Autowired
    private ProductService productService;

    @Override
    public UserScoreEntity calUserScore(String userId) throws IOException {


        return null;
    }

    @Override
    public List<ProductScoreEntity> getProductScore(UserScoreEntity userScoreEntity) {
        return null;
    }

    @Override
    public List<ProductScoreEntity> getTopRankProduct(String userId) throws IOException {
        return null;
    }

    @Override
    public List<ProductEntity> getTopProductFrom(List<String> products) {
        return null;
    }

    private Double[] calColor(String userId) throws Exception {
        int red = getValue(userId, "red");
        int green = getValue(userId, "green");
        int black = getValue(userId, "black");
        int brown = getValue(userId, "brown");
        int grey = getValue(userId, "grey");

        return getPercent(red, green, black, brown, grey);
    }

    private int getValue(String userId, String valueName) throws IOException {
        String value = HbaseClient.getData("user", userId, "style", valueName);
        int res = 0;
        if(null != value){
            res = Integer.valueOf(value);
        }

        return res;
    }

    private Double[] getPercent(int... v){
        int size = v.length;
        double total = 0.0;
        Double[] res = new Double[size];
        for(int i = 0; i < size; i++){
            total += v[i];
        }
        if(total == 0){
            for(int j = 0; j < size; j++){
                res[j] = 0.0;
            }
            return res;
        }
        for(int i = 0; i< size; i++){
            res[i] = v[i] / total;
        }

        return res;
    }
}

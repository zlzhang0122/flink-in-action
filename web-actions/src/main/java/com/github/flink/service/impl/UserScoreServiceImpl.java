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
import java.util.ArrayList;
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
        Double[] colors = calColor(userId);
        Double[] countrys = calcCountry(userId);
        Double[] styles = calcStyle(userId);

        UserScoreEntity userScoreEntity = new UserScoreEntity();
        userScoreEntity.setUserId(userId);
        userScoreEntity.setColor(colors);
        userScoreEntity.setCountry(countrys);
        userScoreEntity.setStyle(styles);

        return userScoreEntity;
    }

    @Override
    public List<ProductScoreEntity> getProductScore(UserScoreEntity userScoreEntity) {
        List<ProductScoreEntity> res = new ArrayList<>();
        List<ProductEntity> productEntityList = getTopProductFrom(redisClient.getTopList(10));
        int i = 0;
        for(ProductEntity entity : productEntityList){
            if(null != entity){
                ProductScoreEntity scoreEntity = new ProductScoreEntity();
                double v = calcProduct(userScoreEntity, entity);
                scoreEntity.setScore(v);
                scoreEntity.setRank(i++);
                scoreEntity.setProductEntity(entity);

                res.add(scoreEntity);
            }
        }

        return res;
    }

    @Override
    public List<ProductScoreEntity> getTopRankProduct(String userId) throws IOException {
        UserScoreEntity userScoreEntity = calUserScore(userId);
        return getProductScore(userScoreEntity);
    }

    @Override
    public List<ProductEntity> getTopProductFrom(List<String> products) {
        List<ProductEntity> top = new ArrayList<>();
        for(String id : products){
            ProductEntity entity;
            if(null != id){
                entity = productService.selectById(id);
                top.add(entity);
            }else{
                top.add(null);
            }
        }

        return top;
    }

    private Double[] calcStyle(String userId) throws IOException {
        int style0 = getValue(userId, "0");
        int style1 = getValue(userId, "1");
        int style2 = getValue(userId, "2");
        int style3 = getValue(userId, "3");
        int style4 = getValue(userId, "4");
        int style5 = getValue(userId, "5");
        int style6 = getValue(userId, "6");

        return getPercent(style0, style1, style2, style3, style4, style5, style6);
    }

    private Double[] calcCountry(String userId) throws IOException {
        int china = getValue(userId, "china");
        int japan = getValue(userId, "japan");
        int korea = getValue(userId, "korea");

        return getPercent(china, japan, korea);
    }

    private Double[] calColor(String userId) throws IOException {
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

    private double calcProduct(UserScoreEntity userScoreEntity, ProductEntity entity){
        String color = entity.getColor();
        String country = entity.getCountry();
        String style = entity.getStyle();

        double val = 0.0;

        switch (color){
            case "red":
                val += userScoreEntity.getColor()[0] == null ? 0 : userScoreEntity.getColor()[0];
                break;
            case "green":
                val += userScoreEntity.getColor()[1] == null ? 0 : userScoreEntity.getColor()[1];
                break;
            case "black":
                val += userScoreEntity.getColor()[2] == null ? 0 : userScoreEntity.getColor()[2];
                break;
            case "brown":
                val += userScoreEntity.getColor()[3] == null ? 0 : userScoreEntity.getColor()[3];
                break;
            case "grey":
                val += userScoreEntity.getColor()[4] == null ? 0 : userScoreEntity.getColor()[4];
                break;
            default:
                val += 0;
                break;
        }

        switch (country){
            case "china":
                val += userScoreEntity.getCountry()[0] == null ? 0 : userScoreEntity.getCountry()[0];
                break;
            case "green":
                val += userScoreEntity.getCountry()[1] == null ? 0 : userScoreEntity.getCountry()[1];
                break;
            case "black":
                val += userScoreEntity.getCountry()[2] == null ? 0 : userScoreEntity.getCountry()[2];
                break;
            default:
                val += 0;
                break;
        }

        switch (style){
            case "0":
                val += userScoreEntity.getStyle()[0] == null ? 0 : userScoreEntity.getStyle()[0];
                break;
            case "1":
                val += userScoreEntity.getStyle()[1] == null ? 0 : userScoreEntity.getStyle()[1];
                break;
            case "2":
                val += userScoreEntity.getStyle()[2] == null ? 0 : userScoreEntity.getStyle()[2];
                break;
            case "3":
                val += userScoreEntity.getStyle()[3] == null ? 0 : userScoreEntity.getStyle()[3];
                break;
            case "4":
                val += userScoreEntity.getStyle()[4] == null ? 0 : userScoreEntity.getStyle()[4];
                break;
            case "5":
                val += userScoreEntity.getStyle()[5] == null ? 0 : userScoreEntity.getStyle()[5];
                break;
            case "6":
                val += userScoreEntity.getStyle()[6] == null ? 0 : userScoreEntity.getStyle()[6];
                break;
            default:
                val += 0;
                break;
        }

        return val;
    }
}

package com.github.flink.client;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * redis操作
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/17 8:12 PM
 */
@Component
public class RedisClient {

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    public String getData(String key) {
        return redisTemplate.opsForValue().get(key);
    }

    public void setData(String key, String value) {
        redisTemplate.opsForValue().set(key, value);
    }

    public List<String> getTopList(int topRange){
        List<String> res = new ArrayList<>();

        for(int i = 0; i < topRange; i++){
            res.add(getData(String.valueOf(i)));
        }

        return res;
    }

    public String getMeter(){
        return getData("meter");
    }
}

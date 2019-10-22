package com.github.flink.service;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 8:47 PM
 */
public interface KafkaService {
    void send(String key, String value);

    String makeLog(String userId, String productId, String action);
}

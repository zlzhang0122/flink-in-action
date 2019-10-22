package com.github.flink.service.impl;

import com.github.flink.service.KafkaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.Date;

/**
 * 向kafka发送日志信息
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/22 9:16 PM
 */
@Service
public class KafkaServiceImpl implements KafkaService {
    private Logger logger = LoggerFactory.getLogger(KafkaService.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private String TOPIC = "flink-recommand-log";

    @Override
    public void send(String key, String value) {
        ListenableFuture<SendResult<String, String>> send = kafkaTemplate.send(TOPIC, key, value);

        send.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("kafka send msg err, ex = {}, topic = {}, data = {}", throwable, TOPIC, value);
            }

            @Override
            public void onSuccess(@Nullable SendResult<String, String> stringStringSendResult) {
                logger.info("kafka send msg success, topic = {}, data = {}", TOPIC, value);
            }
        });
    }

    @Override
    public String makeLog(String userId, String productId, String action) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(userId).append(",");
        stringBuilder.append(productId).append(",");
        stringBuilder.append(getSecondTimestamp(new Date())).append(",");
        stringBuilder.append(action);

        return stringBuilder.toString();
    }

    private static String getSecondTimestamp(Date date){
        if(null == date){
            return "";
        }
        String timestamp = String.valueOf(date.getTime());
        int length = timestamp.length();
        if(length > 3){
            return timestamp.substring(0, length - 3);
        }else{
            return "";
        }
    }
}

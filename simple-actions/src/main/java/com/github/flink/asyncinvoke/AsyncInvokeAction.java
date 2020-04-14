package com.github.flink.asyncinvoke;

import com.alibaba.fastjson.JSONObject;
import com.github.flink.asyncinvoke.model.StockSnapshot;
import com.github.flink.utils.PropertiesUtil;
import com.github.flink.utils.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import java.util.Collections;
import java.util.Properties;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * 异步处理实战
 *
 * @Author: zlzhang0122
 * @Date: 2020/4/14 6:23 PM
 */
public class AsyncInvokeAction {
    private static final Logger logger = LoggerFactory.getLogger(AsyncInvokeAction.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
//        env.enableCheckpointing(10 * 60 * 1000, CheckpointingMode.AT_LEAST_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(20 * 60 * 1000);

        Properties pushToolProperties = PropertiesUtil.getKafkaProperties("async-invoke");
        String topicId = PropertiesUtil.getStrValue("kafka.log.topic.id");
        checkNotNull(topicId, "topicId is not null");

        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<String>(topicId, new SimpleStringSchema(), pushToolProperties);
        consumer.setStartFromGroupOffsets();
        DataStream<String> inStream = env.addSource(consumer).setParallelism(1);

        DataStream<StockSnapshot> pushToolStream = inStream.map((MapFunction<String, StockSnapshot>) s -> {
            StockSnapshot stockSnapshot = null;
            try{
                stockSnapshot = JSONObject.parseObject(s, StockSnapshot.class);
                logger.debug("AsyncInvokeAction stockSnapshot:" + stockSnapshot.toString());
            }catch (Exception e){
                logger.error("AsyncInvokeAction map error", e);
            }

            return stockSnapshot;
        }).returns(StockSnapshot.class).filter((FilterFunction<StockSnapshot>) stockSnapshot -> {
            boolean flag = false;

            try{
                if(stockSnapshot != null){
                    flag = true;

                    String curDateLocal = stockSnapshot.getMdTime();
                    String curDateRemote = TimeUtil.getTodayDayStr("yyyy-MM-dd");
                    if(StringUtils.isNotBlank(curDateLocal) && StringUtils.isNotBlank(curDateRemote)){
                        if(!curDateLocal.equalsIgnoreCase(curDateRemote)){
                            flag = false;
                        }
                    }
                }else{
                    logger.debug("stockSnapshot is null or time is outdate!");
                }
            }catch (Exception e){
                logger.error("AsyncInvokeAction filter first error", e);
            }

            return flag;
        });

        DataStream<String> resStream = AsyncDataStream.unorderedWait(pushToolStream, new RichAsyncFunction<StockSnapshot, String>() {
            public transient ThreadPoolExecutor executor;

            @Override
            public void open(Configuration parameters) throws Exception{
                super.open(parameters);
                executor = new ThreadPoolExecutor(10, 30, 2, TimeUnit.MINUTES, new LinkedBlockingDeque<>());
            }

            @Override
            public void asyncInvoke(StockSnapshot stockSnapshot, ResultFuture<String> resultFuture) throws Exception {
                CompletableFuture.runAsync(() -> {
                    try{
                        Random random = new Random(System.currentTimeMillis());
                        int num = random.nextInt() % 10;
                        String s  = stockSnapshot.toString();
                        Thread.sleep(1000 * num);

                        resultFuture.complete(Collections.singleton(s));
                    }catch (Exception e1){
                        logger.error("time out asyncInvoke, stockSnapshot:" + stockSnapshot);
                    }
                });
            }

            @Override
            public void timeout(StockSnapshot s, ResultFuture<String> resultFuture) throws Exception {
                logger.error("time out, s:" + s);
            }

            @Override
            public void close() throws Exception{
                super.close();
                executor.shutdown();
            }
        }, 5, TimeUnit.MINUTES, 100).returns(String.class);

        //结果发往Kafka
        String sendTopicId = PropertiesUtil.getStrValue("kafka.log.topic.id.demo");
        resStream.addSink(new FlinkKafkaProducer011<>(sendTopicId, new SimpleStringSchema(), pushToolProperties)).setParallelism(1).name("invoke-end");

        //触发执行
        env.execute();
    }
}

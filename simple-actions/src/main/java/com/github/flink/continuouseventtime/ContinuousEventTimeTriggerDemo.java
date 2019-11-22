package com.github.flink.continuouseventtime;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/22 1:30 PM
 */
public class ContinuousEventTimeTriggerDemo {
    public static void main(String[] args){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setMaxParallelism(1);


    }
}

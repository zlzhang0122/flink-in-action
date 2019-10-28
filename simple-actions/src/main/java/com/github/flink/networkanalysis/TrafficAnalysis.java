package com.github.flink.networkanalysis;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 网络流量分析,每5秒统计一次10分钟内访问量最高的10个url,与actionanalysis类似
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/28 7:10 PM
 */
public class TrafficAnalysis {
    private static final Logger logger = LoggerFactory.getLogger(TrafficAnalysis.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


    }
}

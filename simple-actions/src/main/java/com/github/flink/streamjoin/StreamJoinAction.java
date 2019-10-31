package com.github.flink.streamjoin;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

/**
 * 双流join实现
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/31 6:56 PM
 */
public class StreamJoinAction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<String> listA = new ArrayList<>();
        listA.add("2019-08-08 13:00:01.820,000001,10.2");
        listA.add("2019-08-08 13:00:01.260,000001,10.2");
        listA.add("2019-08-08 13:00:02.980,000001,10.1");
        listA.add("2019-08-08 13:00:04.330,000001,10.0");
        listA.add("2019-08-08 13:00:05.570,000001,10.0");
        listA.add("2019-08-08 13:00:05.990,000001,10.0");
        listA.add("2019-08-08 13:00:14.000,000001,10.1");
        listA.add("2019-08-08 13:00:20.000,000001,10.2");
        DataStream<String> dataStreamA = env.fromCollection(listA);

        List<String> listB = new ArrayList<>();
        listA.add("2019-08-08 13:00:01.000,000001,10.2");
        listA.add("2019-08-08 13:00:04.000,000001,10.1");
        listA.add("2019-08-08 13:00:07.000,000001,10.0");
        listA.add("2019-08-08 13:00:16.000,000001,10.1");
        DataStream<String> dataStreamB = env.fromCollection(listB);


    }
}

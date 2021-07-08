package com.github.flink.streaming;

import com.github.flink.model.TrafficRecord;
import com.github.flink.utils.producer.FakeTrafficRecordSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 生成无限流Demo(用于测试)
 *
 * @Author: zlzhang0122
 * @Date: 2021/1/18 4:42 下午
 */
public class InfiniteStream {
    private static final Logger logger = LoggerFactory.getLogger(InfiniteStream.class);

    public static void main(String[] args) throws Exception{
        PrintHelper.helper();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        FakeTrafficRecordSource fakeTrafficRecordSource = new FakeTrafficRecordSource();

        DataStream<TrafficRecord> dataStream = env.addSource(fakeTrafficRecordSource);
        dataStream.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofMillis(5000)));

        DataStream<Tuple2<Integer, Integer>> infiniteStreams = dataStream.map((value) -> new Tuple2<>(value.cityId, 1)).returns(Types.TUPLE(Types.INT, Types.INT))
                .keyBy("0").timeWindow(Time.seconds(5)).sum(1);;

        DataStream<Tuple2<Integer, Integer>> dataStreamForNew = infiniteStreams.map((v) -> {
            logger.info("key0:" + v.f0 + ", value0:" + v.f1);
            System.out.println("key1:" + v.f0 + ", value1:" + v.f1);
            System.err.println("key2:" + v.f0 + ", value2:" + v.f1);
            return v;
        }).returns(Types.TUPLE(Types.INT, Types.INT));
//        dataStreamForNew.print();

        env.execute("Main stream");
    }
}

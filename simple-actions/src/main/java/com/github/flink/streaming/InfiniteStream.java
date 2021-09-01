package com.github.flink.streaming;

import com.github.flink.model.TrafficRecord;
import com.github.flink.utils.producer.FakeTrafficRecordSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
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
//            System.err.println("key0:" + v.f0 + ", value0:" + v.f1);
            return v;
        }).returns(Types.TUPLE(Types.INT, Types.INT));
//        dataStreamForNew.print();

        OutputTag<Tuple2<Integer, Integer>> needSendTag = new OutputTag<Tuple2<Integer, Integer>>("need_send") {};
        SingleOutputStreamOperator<Tuple2<Integer, Integer>> res = dataStreamForNew.process(new ProcessFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
            @Override
            public void processElement(Tuple2<Integer, Integer> integerIntegerTuple2, Context context, Collector<Tuple2<Integer, Integer>> collector) throws Exception {
                if (integerIntegerTuple2.f1 % 2 == 0){
                    collector.collect(integerIntegerTuple2);
                }else {
                    context.output(needSendTag, integerIntegerTuple2);
                }
            }
        });

        res.map((v) -> {
            System.err.println("key1:" + v.f0 + ", value1:" + v.f1);
            return v;
        }).returns(Types.TUPLE(Types.INT, Types.INT));;

        DataStream<Tuple2<Integer, Integer>> needSendDataStream = res.getSideOutput(needSendTag);
        needSendDataStream.map((v) -> {
            System.err.println("key2:" + v.f0 + ", value2:" + v.f1);
            return v;
        }).returns(Types.TUPLE(Types.INT, Types.INT));;

        env.execute("Main stream");
    }
}

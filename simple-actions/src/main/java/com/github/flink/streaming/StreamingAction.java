package com.github.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 15:56
 */
public class StreamingAction {
    private static final Logger logger = LoggerFactory.getLogger(StreamingAction.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env.socketTextStream("localhost", 55555)
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>(){
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] tokens = value.toLowerCase().split("\\W+");
                        for(String token : tokens){
                            if(token.length() > 0){
                                out.collect(new Tuple2<String, Integer>(token, 1));
                            }
                        }
                    }
                }).keyBy(0).timeWindow(Time.seconds(5)).sum(1);

        DataStream<Tuple2<String, Integer>> dataStreamForNew = dataStream.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>(){
                public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                    System.out.println(value.f0 + ":" + value.f1);
                    logger.info(value.f0 + ":" + value.f1);
                    return new Tuple2<String, Integer>(value.f0, value.f1 * 2);
                }});

        dataStreamForNew.print();

        env.execute("Window WordCount");
    }
}

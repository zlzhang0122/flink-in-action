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

import java.util.Random;

/**
 * 不走寻常路的开发(直连kafka，不走flink kafka connector)
 *
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

        DataStream<Tuple2<String, Integer>> dataStreamForNew = dataStream.flatMap(new FlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>(){
                public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                    System.out.println(value.f0 + ":" + value.f1);
                    logger.info(value.f0 + ":" + value.f1);
                    for(int i = 0; i < 100000; i++){
                        Random random = new Random(System.currentTimeMillis());
                        collector.collect(new Tuple2<String, Integer>(value.f0 + random.nextInt(1000000), value.f1 * 2));
                    }
                }});


        DataStream<String> dataStreamRes = dataStreamForNew.map(new KafkaTest());

        dataStreamForNew.print();

        env.execute("Window WordCount");
    }
}

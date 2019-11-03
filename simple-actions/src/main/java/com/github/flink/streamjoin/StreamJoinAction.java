package com.github.flink.streamjoin;

import com.github.flink.streamjoin.functions.UserCustomInnerJoinFunction;
import com.github.flink.streamjoin.functions.UserCustomLeftJoinFunction;
import com.github.flink.streamjoin.functions.UserCustomRightJoinFunction;
import com.github.flink.streamjoin.model.StockSnapshot;
import com.github.flink.streamjoin.model.StockTransaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 双流join实现
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/31 6:56 PM
 */
public class StreamJoinAction {
    private static final Logger logger = LoggerFactory.getLogger(StreamJoinAction.class);

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
        DataStream<StockTransaction> dataStreamA = env.fromCollection(listA).map(new MapFunction<String, StockTransaction>() {
            @Override
            public StockTransaction map(String value) throws Exception {
                String[] s = value.split(",");

                return new StockTransaction(s[0], s[1], Double.parseDouble(s[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StockTransaction>() {
            @Override
            public long extractAscendingTimestamp(StockTransaction element) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                try{
                    Date date = simpleDateFormat.parse(element.getTxTime());
                    return date.getTime();
                }catch (Exception e){
                    logger.error("parse error", e);
                    return Integer.MAX_VALUE;
                }
            }
        });

        List<String> listB = new ArrayList<>();
        listA.add("2019-08-08 13:00:01.000,000001,10.2");
        listA.add("2019-08-08 13:00:04.000,000001,10.1");
        listA.add("2019-08-08 13:00:07.000,000001,10.0");
        listA.add("2019-08-08 13:00:16.000,000001,10.1");
        DataStream<StockSnapshot> dataStreamB = env.fromCollection(listB).map(new MapFunction<String, StockSnapshot>() {
            @Override
            public StockSnapshot map(String value) throws Exception {
                String[] s = value.split(",");

                return new StockSnapshot(s[0], s[1], Double.parseDouble(s[2]));
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<StockSnapshot>() {
            @Override
            public long extractAscendingTimestamp(StockSnapshot element) {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

                try{
                    Date date = simpleDateFormat.parse(element.getMdTime());
                    return date.getTime();
                }catch (Exception e){
                    logger.error("parse error", e);
                    return Integer.MAX_VALUE;
                }
            }
        });

        CoGroupedStreams.WithWindow<StockTransaction, StockSnapshot, String, TimeWindow> joinStream = dataStreamA.coGroup(dataStreamB).where(new KeySelector<StockTransaction, String>() {
            @Override
            public String getKey(StockTransaction value) throws Exception {
                return value.getTxCode();
            }
        }).equalTo(new KeySelector<StockSnapshot, String>() {
            @Override
            public String getKey(StockSnapshot value) throws Exception {
                return value.getMdCode();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(3)));

        joinStream.apply(new UserCustomInnerJoinFunction()).print();
        joinStream.apply(new UserCustomLeftJoinFunction()).print();
        joinStream.apply(new UserCustomRightJoinFunction()).print();

        env.execute("flink table join");
    }
}

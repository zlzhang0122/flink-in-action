package com.github.flink.continuouseventtime;

import com.github.flink.continuouseventtime.model.AreaOrder;
import com.github.flink.continuouseventtime.model.Order;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/22 1:30 PM
 */
public class ContinuousEventTimeTriggerDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

        List<String> sourceList = new ArrayList<>();
        sourceList.add("orderId03,1573874530000,gdsId03,300,beijing");
        sourceList.add("orderId03,1573874740000,gdsId03,300,hanzhou");
        DataStream<String> dataStream = env.fromCollection(sourceList);

        dataStream.map(new MapFunction<String, Order>() {
            @Override
            public Order map(String value) throws Exception {
                String[] arr = value.split(",");

                return new Order(arr[0], Long.parseLong(arr[1]), arr[2], Double.parseDouble(arr[3]), arr[4]);
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Order>(Time.seconds(30)) {
            @Override
            public long extractTimestamp(Order element) {
                return element.getOrderTime();
            }
        }).map(new MapFunction<Order, AreaOrder>() {
            @Override
            public AreaOrder map(Order value) throws Exception {
                return new AreaOrder(value.getAreaId(), value.getAmount());
            }
        }).keyBy("areaId").timeWindow(Time.hours(1))
                .trigger(ContinuousEventTimeTrigger.of(Time.minutes(1)))
                .reduce(new ReduceFunction<AreaOrder>() {
                    @Override
                    public AreaOrder reduce(AreaOrder value1, AreaOrder value2) throws Exception {
                        return new AreaOrder(value1.getAreaId(), value1.getAmount() + value2.getAmount());
                    }
                }).print();

        env.execute("continuous event time trigger");
    }
}

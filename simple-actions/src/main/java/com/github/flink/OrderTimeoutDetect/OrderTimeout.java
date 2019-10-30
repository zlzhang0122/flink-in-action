package com.github.flink.ordertimeoutdetect;

import com.github.flink.ordertimeoutdetect.model.OrderEvent;
import com.github.flink.ordertimeoutdetect.model.OrderResult;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 超时订单检测
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 7:24 PM
 */
public class OrderTimeout {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<OrderEvent> orderEventList = new ArrayList<>();
        orderEventList.add(new OrderEvent(1L, "create", 1558430842L));
        orderEventList.add(new OrderEvent(2L, "create", 1558430843L));
        orderEventList.add(new OrderEvent(2L, "pay", 1558430844L));

        DataStream<OrderEvent> inStream = env.fromCollection(orderEventList).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OrderEvent>() {
            @Override
            public long extractAscendingTimestamp(OrderEvent element) {
                return element.getEventTime() * 1000;
            }
        });

        //定义匹配模式
        Pattern<OrderEvent, ?> orderEventPattern = Pattern.<OrderEvent>begin("begin").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                if("create".equalsIgnoreCase(orderEvent.getEventType())){
                    return true;
                }

                return false;
            }
        }).next("next").where(new IterativeCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent orderEvent, Context<OrderEvent> context) throws Exception {
                if("pay".equalsIgnoreCase(orderEvent.getEventType())){
                    return true;
                }

                return false;
            }
        }).within(Time.minutes(15));

        //定义输出标签
        OutputTag<OrderEvent> orderTimeoutOutput = new OutputTag<OrderEvent>("orderTimeout"){};

        //根据订单id分流,然后匹配模式
        PatternStream patternStream = CEP.pattern(inStream.keyBy("orderId"), orderEventPattern);

        SingleOutputStreamOperator outStream = patternStream.select(orderTimeoutOutput, new PatternTimeoutFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult timeout(Map<String, List<OrderEvent>> map, long l) throws Exception {
                OrderEvent createOrder = map.get("begin").iterator().next();
                return new OrderResult(createOrder.getOrderId(), "timeout");
            }
        }, new PatternSelectFunction<OrderEvent, OrderResult>() {
            @Override
            public OrderResult select(Map<String, List<OrderEvent>> map) throws Exception {
                OrderEvent payOrder = map.get("next").iterator().next();
                return new OrderResult(payOrder.getOrderId(), "success");
            }
        });

        //超时订单
        DataStream<OrderResult> timeOutStream = outStream.getSideOutput(orderTimeoutOutput);

        outStream.print();
        timeOutStream.print();

        env.execute("Order Timeout Detect Job");
    }
}

package com.github.flink.loginfaildetect;

import com.github.flink.loginfaildetect.model.LoginEvent;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 登录异常检测cep
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 6:51 PM
 */
public class LoginFailWithCep {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<LoginEvent> inList = new ArrayList<>();
        inList.add(new LoginEvent(1L, "192.168.0.1", "fail", 1558430842L));
        inList.add(new LoginEvent(1L, "192.168.0.2", "fail", 1558430843L));
        inList.add(new LoginEvent(1L, "192.168.0.3", "fail", 1558430844L));
        inList.add(new LoginEvent(2L, "192.168.10.10", "success", 1558430845L));

        DataStream<LoginEvent> inDataStream = env.fromCollection(inList).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.getEventTime() * 1000;
            }
        });

        //定义匹配模式
        Pattern<LoginEvent, ?> loginFailPattern = Pattern.<LoginEvent>begin("begin").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent o, Context<LoginEvent> context) throws Exception {
                if("fail".equalsIgnoreCase(o.getEventType())){
                    return true;
                }

                return false;
            }
        }).next("next").where(new IterativeCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent loginEvent, Context<LoginEvent> context) throws Exception {
                if("fail".equalsIgnoreCase(loginEvent.getEventType())){
                    return true;
                }

                return false;
            }
        }).within(Time.seconds(2));

        //在数据流中匹配出定义好的模式
        PatternStream<LoginEvent> patternStream = CEP.pattern(inDataStream.keyBy("userId"), loginFailPattern);

        patternStream.select(new PatternSelectFunction<LoginEvent, LoginEvent>() {
            @Override
            public LoginEvent select(Map<String, List<LoginEvent>> map) throws Exception {
                LoginEvent first = map.getOrDefault("begin", null).iterator().next();
                LoginEvent second = map.getOrDefault("next", null).iterator().next();

                return new LoginEvent(first.getUserId(), second.getIp(), second.getEventType());
            }
        }).print();

        env.execute("Login Fail Detect Job");
    }
}

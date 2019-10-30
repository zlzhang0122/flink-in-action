package com.github.flink.LoginFailDetect;

import com.github.flink.LoginFailDetect.model.LoginEvent;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 异常登录检测
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 6:11 PM
 */
public class LoginFail {
    private static final Logger logger = LoggerFactory.getLogger(LoginFail.class);

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<LoginEvent> inList = new ArrayList<>();
        inList.add(new LoginEvent(1L, "192.168.0.1", "fail", 1558430842L));
        inList.add(new LoginEvent(1L, "192.168.0.2", "fail", 1558430843L));
        inList.add(new LoginEvent(1L, "192.168.0.3", "fail", 1558430844L));
        inList.add(new LoginEvent(2L, "192.168.10.10", "success", 1558430845L));

        env.fromCollection(inList).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<LoginEvent>() {
            @Override
            public long extractAscendingTimestamp(LoginEvent element) {
                return element.getEventTime() * 1000;
            }
        }).keyBy("userId").process(new KeyedProcessFunction<Tuple, LoginEvent, LoginEvent>() {
            private ListState<LoginEvent> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                ListStateDescriptor<LoginEvent> descriptor = new ListStateDescriptor<LoginEvent>("saved-login", LoginEvent.class);
                listState = getRuntimeContext().getListState(descriptor);
            }

            @Override
            public void processElement(LoginEvent value, Context ctx, Collector<LoginEvent> out) throws Exception {
                if("fail".equalsIgnoreCase(value.getEventType())){
                    listState.add(value);
                }
                ctx.timerService().registerEventTimeTimer(value.getEventTime() + 2 * 1000);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<LoginEvent> out) throws Exception {
                List<LoginEvent> loginEventList = new ArrayList<LoginEvent>();
                for(LoginEvent loginEvent : listState.get()){
                    loginEventList.add(loginEvent);
                }
                inList.clear();
                if(loginEventList.size() > 1){
                    out.collect(loginEventList.get(0));
                }
            }
        }).print();

        env.execute("Login Fail Detect Job");
    }
}

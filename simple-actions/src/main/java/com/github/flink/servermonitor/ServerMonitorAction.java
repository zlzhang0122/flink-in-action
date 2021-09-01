package com.github.flink.servermonitor;

import com.github.flink.servermonitor.model.ServerMonitor;
import com.github.flink.utils.TimeUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 服务器上下线监控报警
 *
 * @Author: zlzhang0122
 * @Date: 2019/11/11 4:05 PM
 */
public class ServerMonitorAction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<String> demoDatas = new ArrayList<>();
        demoDatas.add("1,true,2019-11-11 13:00:01.820");
        demoDatas.add("1,true,2019-11-11 13:00:09.730");
        demoDatas.add("1,true,2019-11-11 13:02:09.870");
        demoDatas.add("2,false,2019-11-11 13:02:17.290");
        demoDatas.add("1,false,2019-11-11 13:04:08.250");
        demoDatas.add("2,true,2019-11-11 13:04:11.730");
        demoDatas.add("1,false,2019-11-11 13:09:21.970");
        DataStream<String> source = env.fromCollection(demoDatas);

        DataStream<String> dataStream = source.map(new MapFunction<String, ServerMonitor>() {
            @Override
            public ServerMonitor map(String value) throws Exception {
                String[] arr = value.split(",");
                Date date = TimeUtil.getDateFromString(arr[2], "yyyy-MM-dd HH:mm:ss.SSS");

                if(date != null){
                    return new ServerMonitor(arr[0], arr[1], date);
                }else{
                    return null;
                }
            }
        }).filter((FilterFunction<ServerMonitor>) value -> {
            if(value == null){
                return false;
            }

            return true;
        }).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<ServerMonitor>() {
            private Long currentMaxTimestamp = 0L;

            @Override
            public long extractTimestamp(ServerMonitor element, long previousElementTimestamp) {
                currentMaxTimestamp = Math.max(element.getDate().getTime(), currentMaxTimestamp);
                return currentMaxTimestamp;
            }

            @Nullable
            @Override
            public Watermark checkAndGetNextWatermark(ServerMonitor lastElement, long extractedTimestamp) {
                return new Watermark(currentMaxTimestamp - 60000);
            }
        }).keyBy("serverId").process(new KeyedProcessFunction<Tuple, ServerMonitor, String>() {
            ValueState<String> serverState;
            ValueState<Long> timeState;
            MapState<String, String> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> serverDesc = new ValueStateDescriptor("server-state", String.class);
                serverState = getRuntimeContext().getState(serverDesc);
                ValueStateDescriptor<Long> timeDesc = new ValueStateDescriptor("time-state", Long.class);
                timeState = getRuntimeContext().getState(timeDesc);
                MapStateDescriptor<String, String> mapTest = new MapStateDescriptor("map-state", String.class,String.class);
                mapState = getRuntimeContext().getMapState(mapTest);
            }

            @Override
            public void processElement(ServerMonitor value, Context ctx, Collector<String> out) throws Exception {
                System.out.println("value:" + value);
                if("false".equalsIgnoreCase(value.getFlag())){
                    long monitorTime = ctx.timerService().currentWatermark() + 300000;
                    timeState.update(monitorTime);
                    serverState.update(value.getServerId());

                    ctx.timerService().registerEventTimeTimer(monitorTime);
                }

                if("true".equalsIgnoreCase(value.getFlag()) && timeState.value() != null && timeState.value() != -1){
                    ctx.timerService().deleteEventTimeTimer(timeState.value());
                    timeState.update(-1L);
                }

                mapState.put(value.getDate().toString(), "test");
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                if(timestamp == timeState.value()){
//                    ctx.timerService().deleteEventTimeTimer(timestamp);
                    long monitorTimeNew = timestamp + 300000;
                    timeState.update(monitorTimeNew);
//                    ctx.timerService().registerEventTimeTimer(monitorTimeNew);
                    System.out.println("alert:" + serverState.value() + " is offline, please restart!");
                    out.collect("alert:" + serverState.value() + " is offline");
                }
            }
        });

        dataStream.print();

        env.execute("server monitor");
    }
}

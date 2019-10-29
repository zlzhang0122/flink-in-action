package com.github.flink.networkanalysis;

import com.github.flink.networkanalysis.functions.CountAgg;
import com.github.flink.networkanalysis.functions.TopNHotUrls;
import com.github.flink.networkanalysis.functions.WindowResultFunction;
import com.github.flink.networkanalysis.model.ApacheLogEvent;
import com.github.flink.networkanalysis.model.UrlViewCount;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * 网络流量分析,每5秒统计一次10分钟内访问量最高的10个url,与actionanalysis类似
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/28 7:10 PM
 */
public class TrafficAnalysis {
    private static final Logger logger = LoggerFactory.getLogger(TrafficAnalysis.class);

    private static final String INPUT_NAME = "apache.log";

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<String> logs = new ArrayList<>();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(INPUT_NAME);
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String str = "";
            while(!"".equals(str = bufferedReader.readLine())){
                logs.add(str);
            }
        } catch (IOException e) {
            System.err.println("日志文件加载失败!");
            e.printStackTrace();
        }

        DataStream<String> inDataStream = env.fromCollection(logs);

        inDataStream.map(new MapFunction<String, ApacheLogEvent>() {
            @Override
            public ApacheLogEvent map(String value) throws Exception {
                if(StringUtils.isNoneBlank(value)){
                    String[] arr = value.split(" ");

                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                    Long timestamp = simpleDateFormat.parse(arr[3]).getTime();
                    return new ApacheLogEvent(arr[0], arr[2], timestamp, arr[5], arr[6]);
                }

                return null;
            }
        }).filter(new FilterFunction<ApacheLogEvent>() {
            @Override
            public boolean filter(ApacheLogEvent value) throws Exception {
                if(value == null){
                    return false;
                }

                return true;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<ApacheLogEvent>(Time.milliseconds(1000)) {
            @Override
            public long extractTimestamp(ApacheLogEvent element) {
                return element.getEventTime();
            }
        }).keyBy("url").timeWindow(Time.minutes(10), Time.seconds(5))
                .aggregate(new CountAgg(), new WindowResultFunction())
                .keyBy(1).process(new TopNHotUrls(5)).print();

        env.execute("Traffic Analysis Job");
    }
}

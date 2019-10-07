package com.github.flink;

import com.github.flink.function.map.LogMapFunction;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/12 18:20
 */
public class LogTask {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = PropertiesUtil.getKafkaProperties("log");
        FlinkKafkaManager manager = new FlinkKafkaManager("flink-recommand-log", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildString();
        consumer.setStartFromEarliest();

        DataStream<String> dataStream = env.addSource(consumer);
        dataStream.map(new LogMapFunction());

        env.execute("log to hbase");
    }
}

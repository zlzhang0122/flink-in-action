package com.github.flink;

import com.github.flink.function.map.ProductPortraitMapFunction;
import com.github.flink.function.map.UserPortraitMapFunction;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 用户画像任务
 *
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午11:31
 */
public class UserPortraitTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = PropertiesUtil.getKafkaProperties("userPortrait");
        FlinkKafkaManager<String> manager = new FlinkKafkaManager<>("flink-recommand-log", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildString();
        consumer.setStartFromEarliest();

        DataStream<String> stream = env.addSource(consumer);
        stream.map(new UserPortraitMapFunction());

        env.execute("user portrait");
    }
}
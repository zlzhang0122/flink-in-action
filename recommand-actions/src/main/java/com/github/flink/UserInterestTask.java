package com.github.flink;

import com.github.flink.function.map.GetLogFunction;
import com.github.flink.function.map.UserHistoryWithInterestMapFunction;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午11:18
 */
public class UserInterestTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = PropertiesUtil.getKafkaProperties("interest");
        FlinkKafkaManager<String> manager = new FlinkKafkaManager<>("con", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildString();

        DataStream<String> stream = env.addSource(consumer);
        stream.map(new GetLogFunction()).keyBy("userId").map(new UserHistoryWithInterestMapFunction());

        env.execute("user interest history");
    }
}

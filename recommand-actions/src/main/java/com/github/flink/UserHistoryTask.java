package com.github.flink;

import com.github.flink.function.map.UserHistoryMapFunction;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 浏览历史记录任务
 *
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午10:47
 */
public class UserHistoryTask {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = PropertiesUtil.getKafkaProperties("flink-history");
        FlinkKafkaManager<String> manager = new FlinkKafkaManager<>("flink-recommand-log", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildString();
        consumer.setStartFromEarliest();

        DataStreamSource<String> dataStream = env.addSource(consumer);
        dataStream.map(new UserHistoryMapFunction());

        env.execute("user product history");
    }
}

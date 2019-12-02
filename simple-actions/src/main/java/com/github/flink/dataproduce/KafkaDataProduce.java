package com.github.flink.dataproduce;

import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/16 下午10:37
 */
public class KafkaDataProduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = environment.addSource(new KafkaDataSource());

        String brokerList = PropertiesUtil.getStrValue("kafka.bootstrap.servers");
        String topic = PropertiesUtil.getStrValue("kafka.log.topic.id.demo");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", brokerList);
        dataStream.addSink(new FlinkKafkaProducer(topic, new SimpleStringSchema(), properties)).name("produce");

        environment.execute("kafka data produce");
    }
}

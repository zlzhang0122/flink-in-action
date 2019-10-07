package com.github.flink;

import com.github.flink.function.map.ProductPortraitMapFunction;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/13 下午8:22
 */
public class ProductPortraitTask {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = PropertiesUtil.getKafkaProperties("product-portrait");
        FlinkKafkaManager<String> manager = new FlinkKafkaManager<>("flink-recommand-product-portrait", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildString();

        DataStream<String> dataStream = env.addSource(consumer);
        dataStream.map(new ProductPortraitMapFunction());

        env.execute("product portrait to hbase!");
    }
}

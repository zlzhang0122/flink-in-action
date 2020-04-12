package com.github.flink.streaming;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: zlzhang0122
 * @Date: 2020/4/12 10:21 AM
 */
public class KafkaTest extends RichMapFunction<Tuple2<String, Integer>, String> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaTest.class);

    private Producer<String, String> producer;

    private ExecutorService executorService;

    @Override
    public void open(Configuration parameters) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 1024 * 1024 * 10);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
        executorService = Executors.newFixedThreadPool(5);
    }

    @Override
    public String map(Tuple2<String, Integer> tuple2) throws Exception {
        executorService.submit(() -> {
            final String json = tuple2.toString();
            final ProducerRecord<String, String> record = new ProducerRecord<>("kafka-test", json);
            try {
                producer.send(record);
            }catch (Exception e) {
                logger.error("send kafka error!");
            }
            logger.info("send record " + record);
        });

        return "";
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }
}
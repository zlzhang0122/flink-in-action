package com.github.flink.retractstream;

import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2020/1/6 10:46 PM
 */
public class RetractWordCount {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(env);

        Properties properties = PropertiesUtil.getKafkaProperties("retract");
        FlinkKafkaManager manager = new FlinkKafkaManager<>("topic-1", properties);
        FlinkKafkaConsumer consumer = manager.buildStringConsumer();
        DataStream ds = env.addSource(consumer).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return new Tuple2<String, Integer>(value, 1);
            }
        });

        tabEnv.registerDataStream("table1", ds, "word, cnt");

        Table table = tabEnv.sqlQuery("select word, sum(cnt) from table1 group by word");
        TableSink tableSink = new MyRetractStreamTableSink();

        env.execute("retract word count");
    }

}

package com.github.flink.rdbms;

import com.alibaba.fastjson.JSONObject;
import com.github.flink.rdbms.commons.Agg;
import com.github.flink.rdbms.commons.CommonBean;
import com.github.flink.rdbms.commons.FlatMap;
import com.github.flink.rdbms.commons.MessageWaterEmitter;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 16:58
 */
public class RdbmsAction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = PropertiesUtil.getKafkaProperties("flink-group");
        FlinkKafkaManager manager = new FlinkKafkaManager("user_behavior", properties);
        //用JsonObject 反序列化接收kafka
        FlinkKafkaConsumer<JSONObject> consumer = manager.build(JSONObject.class);
        //从最新的消息开始接收
        consumer.setStartFromLatest();
        //设置watermark和time
        consumer.assignTimestampsAndWatermarks(new MessageWaterEmitter());
        //获得DataStream
        DataStream<JSONObject> messageStream = env.addSource(consumer);
        //转化为pojo
        DataStream<CommonBean> bean2DataStream = messageStream.map(new FlatMap());

//        bean2DataStream.addSink(new JdbcWriter()); //输出函数

        bean2DataStream.keyBy(CommonBean::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))//基于event time的窗口
                .allowedLateness(Time.seconds(0)) //允许数据延迟多长时间,谨慎使用,迟到的数据会导致出现重复数据
                .aggregate(new Agg()) //聚合函数，这里也可以参照demo2用reduce函数
                .addSink(new JdbcWriter()); //输出函数

        try {
            env.execute("Rdbms action");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}


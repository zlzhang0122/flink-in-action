package com.github.flink.actionanalysis;

import com.alibaba.fastjson.JSONObject;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/27 8:16 PM
 */
public class HotItemsAction {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = PropertiesUtil.getKafkaProperties("user-behavior");
        FlinkKafkaManager manager = new FlinkKafkaManager("user_behavior", properties);

        FlinkKafkaConsumer<JSONObject> consumer = manager.buildConsumer(JSONObject.class);
        //从最近的消息开始处理
        consumer.setStartFromLatest();

        DataStreamSource<JSONObject> inStream = env.addSource(consumer);
        inStream.map(new MapFunction<JSONObject, UserBehavior>() {

            @Override
            public UserBehavior map(JSONObject value) throws Exception {
                UserBehavior userBehavior = new UserBehavior();

                if(value != null){
                    userBehavior.setUserId(value.getLong("user_id"));
                    userBehavior.setItemId(value.getLong("item_id"));
                    userBehavior.setCategoryId(value.getInteger("category_id"));
                    userBehavior.setBehavior(value.getString("behavior"));

                    String ts = value.getString("ts");
                    Long tsLong = new Date().getTime();
                    if(StringUtils.isNoneBlank(ts)){
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                        Date date = format.parse(ts);

                        tsLong = date.getTime();
                    }
                    userBehavior.setTimestamp(tsLong);
                }

                return userBehavior;
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
            @Override
            public long extractAscendingTimestamp(UserBehavior element) {
                return element.getTimestamp() * 1000;
            }
        }).print();

        env.execute("Hot items action");
    }
}

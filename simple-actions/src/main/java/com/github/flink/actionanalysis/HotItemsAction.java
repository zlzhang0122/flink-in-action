package com.github.flink.actionanalysis;

import com.alibaba.fastjson.JSONObject;
import com.github.flink.actionanalysis.functions.CountAgg;
import com.github.flink.actionanalysis.functions.TimestampExtractor;
import com.github.flink.actionanalysis.functions.WindowResultFunction;
import com.github.flink.actionanalysis.model.UserBehavior;
import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 热门商品统计
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/27 8:16 PM
 */
public class HotItemsAction {
    private static final Logger logger = LoggerFactory.getLogger(HotItemsAction.class);

    private static final Integer maxLaggedTime = 5;

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
        }).assignTimestampsAndWatermarks(new TimestampExtractor(maxLaggedTime))
                .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        if(StringUtils.isNoneBlank(value.getBehavior()) && "pv".equals(value.getBehavior())){
                            return false;
                        }else{
                            return true;
                        }
                    }
                }).keyBy("itemId").timeWindow(Time.minutes(60), Time.minutes(5))
                .aggregate(new CountAgg(), new WindowResultFunction());

        env.execute("Hot items action");
    }
}

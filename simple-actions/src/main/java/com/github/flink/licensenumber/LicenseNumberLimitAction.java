package com.github.flink.licensenumber;

import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import com.github.flink.utils.TimeUtil;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/26 2:56 PM
 */
public class LicenseNumberLimitAction {
    private static final String TIME_RULE = "07:00-09:00,16:30-18:30";

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(180000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints"));

        Properties properties = PropertiesUtil.getKafkaProperties("flink-group");
        FlinkKafkaManager manager = new FlinkKafkaManager("license-number-limit", properties);
        FlinkKafkaConsumer consumer = manager.buildString();
        consumer.setStartFromLatest();

        DataStream<String> inStream = env.addSource(consumer);

        DataStream<JSONObject> outStream = inStream.map(new MapFunction<String, Tuple4<String, String, String, String>>() {
            @Override
            public Tuple4<String, String, String, String> map(String value) throws Exception {
                return getInput(value);
            }
        }).filter(new FilterFunction<Tuple4<String, String, String, String>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, String> value) throws Exception {
                if(value.f0 == null && value.f1 == null && value.f2 == null && value.f3 == null){
                    return false;
                }else {
                    return true;
                }
            }
        }).flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, JSONObject>() {
            @Override
            public void flatMap(Tuple4<String, String, String, String> value, Collector<JSONObject> out) throws Exception {

            }
        });
    }

    private static Tuple4<String, String, String, String> getInput(String inStr) throws Exception{
        JSONParser jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);
        JSONObject jsonObject = (JSONObject) jsonParser.parse(inStr);

        //区域位置
        String pointId = jsonObject.get("point_id").toString();
        //经过时间
        String passTime = jsonObject.get("pass_time").toString();
        //车牌类型
        String licenseType = jsonObject.get("license_type").toString();
        //车牌号
        String licenseNum = jsonObject.get("license_num").toString();

        Boolean flag = false;
        try{
            if(TimeUtil.isLicenseNumberLimitTime(TimeUtil.getTimeMillis(passTime), TIME_RULE)){
                return new Tuple4<>(pointId, passTime, licenseType, licenseNum);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return new Tuple4<>(null, null, null, null);
    }
}

package com.github.flink.licensenumber;

import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import com.github.flink.utils.TimeUtil;
import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 违反车牌限号汇总系统(示例)
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/26 2:56 PM
 */
public class LicenseNumberLimitAction {
    private static final Logger logger = LoggerFactory.getLogger(LicenseNumberLimitAction.class);

    private static final String TIME_RULE = "07:00-09:00,16:30-18:30";

    //1海淀,2朝阳,3东城,4西城,5昌平
    private static final List<String> TEST_AREA = new ArrayList<String>(Arrays.asList("1_1","2_2","3_2", "4_1", "5_3",
        "6_4", "7_3", "8_3", "9_5", "10_2"));

    //重点区域监控
    private static final String KEY_AREA_1 = "{\"areaName\":\"海淀区\",\"pointId\":\"1\",\"inOut\":\"9\",\"area_id\":\"1\"}";
    private static final String KEY_AREA_2 = "{\"areaName\":\"朝阳区\",\"pointId\":\"2\",\"inOut\":\"10\",\"area_id\":\"2\"}";

    //限行规则
    private static final String LIMIT_RULE = "1:1_6,2:2_7,3:3_8,4:4_9,5:5_0";

    private static final Integer maxLaggedTime = 5;

    public static void main(String args[]) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //cp配置
        env.enableCheckpointing(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setCheckpointTimeout(180000);
        env.setStateBackend(new FsStateBackend("hdfs:///flink/checkpoints"));

        Properties properties = PropertiesUtil.getKafkaProperties("flink-group");
        FlinkKafkaManager manager = new FlinkKafkaManager("license-number-limit-source", properties);
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
        }).flatMap(new FlatMapFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, String>>() {
            @Override
            public void flatMap(Tuple4<String, String, String, String> value, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                logger.info("flatmap:" + value.f0 + " " + value.f1 + " " + value.f2 + " " + value.f3);

                String[] areas = getAreaId(TEST_AREA.iterator(), value.f0);
                String pointId = value.f0;
                for(String s : areas){
                    JSONParser jsonParser = new JSONParser(JSONParser.DEFAULT_PERMISSIVE_MODE);

                    JSONObject jsonObject = null;
                    if("1".equals(s)){
                        jsonObject = (JSONObject) jsonParser.parse(KEY_AREA_1);
                    }else if("2".equals(s)){
                        jsonObject = (JSONObject) jsonParser.parse(KEY_AREA_2);
                    }

                    if(null == jsonObject){
                        out.collect(new Tuple5<>(null, null, null, null, null));
                    }else{
                        String areaId = jsonObject.get("area_id").toString();

                        out.collect(new Tuple5<>(areaId, pointId, value.f1, value.f2, value.f3));
                    }
                }
            }
        }).filter(new FilterFunction<Tuple5<String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple5<String, String, String, String, String> value) throws Exception {
                if(value.f0 == null && value.f1 == null && value.f2 == null && value.f3 == null && value.f4 == null){
                    return false;
                }else {
                    return true;
                }
            }
        }).map(new MapFunction<Tuple5<String, String, String, String, String>, Tuple6<String, String, String, String, Long, String>>() {
            @Override
            public Tuple6<String, String, String, String, Long, String> map(Tuple5<String, String, String, String, String> value) throws Exception {
                long passTime = TimeUtil.getTimeMillis(value.f2);
                if(passTime - System.currentTimeMillis() > 0){
                    passTime = System.currentTimeMillis();
                }

                String areaId = value.f0;
                String licenseType = value.f3;
                String licenseNum = value.f4;

                String limitRules = "";
                try{
                    String[] limitRulesArray = getLimitRule(passTime);
                    String limitOne = "", limitTwo = "";
                    if(limitRulesArray.length >= 2){
                        limitOne = limitRulesArray[0];
                        limitTwo = limitRulesArray[1];
                    }
                    limitRules = limitOne + "_" + limitTwo;

                    Boolean flag = false;
                    if(isLimitNumber(licenseNum, passTime)){
                        flag = true;
                    }

                    if(flag){
                        return new Tuple6<String, String, String, String, Long, String>(areaId, licenseType, licenseNum, limitRules.substring(5, 8), passTime, value.f1);
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }

                return new Tuple6<String, String, String, String, Long, String>(null, null,null, null, 0L, null);
            }
        }).assignTimestampsAndWatermarks(new TimestampExtractor(maxLaggedTime))
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple6<String, String, String, String, Long, String>>() {
                    @Override
                    public Tuple6<String, String, String, String, Long, String> reduce(Tuple6<String, String, String, String, Long, String> value1, Tuple6<String, String, String, String, Long, String> value2) throws Exception {
                        return new Tuple6<String, String, String, String, Long, String>(value1.f0, value1.f1, value1.f2, value1.f3, value1.f4, value1.f5);
                    }
                })
                .map(new MapFunction<Tuple6<String, String, String, String, Long, String>, JSONObject>() {
                    @Override
                    public JSONObject map(Tuple6<String, String, String, String, Long, String> value) throws Exception {
                        logger.info("toJson:" + value.f0 + " " + value.f1 + " " + value.f2 + " " + value.f3 + " " + value.f4 + " " + value.f5);

                        JSONObject jsonObject = new JSONObject();
                        String passTime = TimeUtil.milliSecondToTimestampString(value.f4);
                        jsonObject.put("area_id", value.f0);
                        jsonObject.put("license_type", value.f1);
                        jsonObject.put("license_num", value.f2);
                        jsonObject.put("limit_rules", value.f3);
                        jsonObject.put("passing_time", value.f4);
                        jsonObject.put("point_id", value.f5);

                        logger.info("----" + jsonObject.toString());
                        return jsonObject;
                    }
                });

        //写回到kafka目标topic
        FlinkKafkaProducer producer = new FlinkKafkaProducer("license-number-limit-target", new SimpleStringSchema(), properties);
        outStream.addSink(producer);

        //执行
        env.execute("LicenceNumberLimit");
    }

    /**
     * 解析数据,如果违反限号时间则返回记录,否则返回null
     *
     * @param inStr
     * @return
     * @throws Exception
     */
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

        try{
            if(TimeUtil.isLicenseNumberLimitTime(TimeUtil.getTimeMillis(passTime), TIME_RULE)){
                return new Tuple4<>(pointId, passTime, licenseType, licenseNum);
            }
        }catch (Exception e){
            e.printStackTrace();
        }

        return new Tuple4<>(null, null, null, null);
    }

    /**
     * 根据pointId获取区域id
     *
     * @param pointAndArea
     * @param pointId
     * @return
     */
    private static String[] getAreaId(Iterator<String> pointAndArea, String pointId){
        List<String> list = new ArrayList<>();
        while(pointAndArea.hasNext()){
            String item = pointAndArea.next();
            while(StringUtils.isNoneBlank(item)){
                String[] arr = item.split("_");
                if(arr[0].equals(pointId)){
                    list.add(arr[1]);
                }
            }
        }

        return list.toArray(new String[0]);
    }

    /**
     * 获取当日限行规则
     *
     * @param passingTime
     * @return
     */
    private static String[] getLimitRule(Long passingTime) throws Exception{
        List<String> ret = new ArrayList<>();

        String time = TimeUtil.milliSecondToTimestampString(passingTime);
        int flag = TimeUtil.dayOfWeek(time);

        String limitRule = LIMIT_RULE;
        Map<String, String> map = new HashedMap();
        String[] limitRules = limitRule.split(",");
        for(int i = 0; i < limitRules.length; i++){
            String item = limitRules[i];

            String[] rules = item.split(":");
            map.put(rules[0], rules[1]);
        }

        String rule = map.get(flag);
        String[] ele = rule.split("_");
        ret.add(ele[0]);
        ret.add(ele[1]);

        return ret.toArray(new String[0]);
    }

    /**
     * 判断号牌是否限行
     *
     * @param licenseNumber
     * @param passingTime
     * @return
     * @throws Exception
     */
    private static Boolean isLimitNumber(String licenseNumber, Long passingTime) throws Exception{
        String lastNum = getLastIntNumber(licenseNumber);
        String[] limitRulesArray = getLimitRule(passingTime);

        String str = "";
        for(int i = 0; i< limitRulesArray.length; i++){
            str += limitRulesArray[i];
        }

        if(str.contains(lastNum)){
            return true;
        }else {
            return false;
        }
    }

    /**
     * 获取车牌最后一位数字
     *
     * @param licenseNumber
     * @return
     */
    private static String getLastIntNumber(String licenseNumber){
        String regEx = "[^0-9]";
        Pattern pattern = Pattern.compile(regEx);
        Matcher matcher = pattern.matcher(licenseNumber);
        String result = matcher.replaceAll("").trim();

        return result.substring(result.length() - 1);
    }
}

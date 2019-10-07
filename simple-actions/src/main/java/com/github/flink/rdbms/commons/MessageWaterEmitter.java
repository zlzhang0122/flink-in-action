package com.github.flink.rdbms.commons;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 17:28
 */
public class MessageWaterEmitter implements AssignerWithPunctuatedWatermarks<JSONObject> {
    private static final Logger logger = LoggerFactory.getLogger(MessageWaterEmitter.class);

    public Watermark checkAndGetNextWatermark(JSONObject lastElement, long extractedTimestamp) {
        long date = getDate(lastElement);
        if (date > 0L) {
            return new Watermark(date);
        }

        return null;
    }

    public long extractTimestamp(JSONObject element, long previousElementTimestamp) {
        return getDate(element);
    }

    /**
     * 获取时间
     *
     * @param ele
     * @return
     */
    public long getDate(JSONObject ele){
        long dateLong = 0L;

        try{
            if(ele != null && ele.size() > 0){
                if(ele.containsKey("ts")){
                    String dateStr = ele.getString("ts");

                    if(StringUtils.isNoneBlank(dateStr)){
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                        Date date = format.parse(dateStr);

                        dateLong = date.getTime();
                    }
                }
            }
        }catch (Exception e){
            logger.error("MessageWaterEmitter getDate error", e);
            System.out.println("MessageWaterEmitter getDate error!");
        }

        return dateLong;
    }
}

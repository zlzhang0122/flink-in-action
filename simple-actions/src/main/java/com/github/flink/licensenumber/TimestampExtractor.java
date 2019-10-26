package com.github.flink.licensenumber;

import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 时间和水位线生成类
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/26 11:16 PM
 */
public class TimestampExtractor implements AssignerWithPunctuatedWatermarks<Tuple6<String, String, String, String, Long, String>> {

    private Long currentMaxTimestamp = 0L;

    private Integer maxLaggedTime = 0;

    public TimestampExtractor(Integer maxLaggedTime) {
        this.maxLaggedTime = maxLaggedTime;
    }

    /**
     * 得到当前水位值
     *
     * @param lastElement
     * @param extractedTimestamp
     * @return
     */
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple6<String, String, String, String, Long, String> lastElement, long extractedTimestamp) {
        return new Watermark(currentMaxTimestamp - maxLaggedTime * 1000);
    }

    /**
     * 抽取事件时间
     *
     * @param element
     * @param previousElementTimestamp
     * @return
     */
    @Override
    public long extractTimestamp(Tuple6<String, String, String, String, Long, String> element, long previousElementTimestamp) {
        Long timestamp = element.f4;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);

        return timestamp;
    }
}

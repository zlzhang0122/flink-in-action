package com.github.flink.actionanalysis.functions;

import com.github.flink.actionanalysis.model.UserBehavior;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * 时间和水位线生成类
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/26 11:16 PM
 */
public class TimestampExtractor implements AssignerWithPunctuatedWatermarks<UserBehavior> {

    private Long currentMaxTimestamp = 0L;

    private Integer maxLaggedTime = 10;

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
    public Watermark checkAndGetNextWatermark(UserBehavior lastElement, long extractedTimestamp) {
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
    public long extractTimestamp(UserBehavior element, long previousElementTimestamp) {
        Long timestamp = element.getTimestamp();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);

        return timestamp;
    }
}

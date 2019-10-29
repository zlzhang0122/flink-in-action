package com.github.flink.networkanalysis.functions;

import com.github.flink.actionanalysis.model.UserBehavior;
import com.github.flink.networkanalysis.model.ApacheLogEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 聚合函数
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/27 8:51 PM
 */
public class CountAgg implements AggregateFunction<ApacheLogEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(ApacheLogEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

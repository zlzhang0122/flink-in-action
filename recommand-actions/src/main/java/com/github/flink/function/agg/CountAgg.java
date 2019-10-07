package com.github.flink.function.agg;

import com.github.flink.domain.LogEntity;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/16 下午10:15
 */
public class CountAgg implements AggregateFunction<LogEntity, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(LogEntity logEntity, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

package com.github.flink.actionanalysis.functions;

import com.github.flink.actionanalysis.model.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * 聚合函数
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/27 8:51 PM
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior value, Long accumulator) {
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

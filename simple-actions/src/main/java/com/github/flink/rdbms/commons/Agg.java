package com.github.flink.rdbms.commons;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 18:53
 */
public class Agg implements AggregateFunction<CommonBean, Tuple2<CommonBean, Long>, Tuple2<CommonBean, Long>> {

    @Override
    public Tuple2<CommonBean, Long> createAccumulator() {
        return new Tuple2<CommonBean, Long>();
    }

    @Override
    public Tuple2<CommonBean, Long> add(CommonBean bean3, Tuple2<CommonBean, Long> bean2LongTuple2) {
        CommonBean bean = bean2LongTuple2.f0;
        Long count = bean2LongTuple2.f1;
        if (bean == null) {
            bean = bean3;
        }
        if (count == null) {
            count = 1L;
        } else {
            count++;
        }

        return new Tuple2<>(bean, count);
    }

    @Override
    public Tuple2<CommonBean, Long> getResult(Tuple2<CommonBean, Long> bean2LongTuple2) {
        return bean2LongTuple2;
    }

    @Override
    public Tuple2<CommonBean, Long> merge(Tuple2<CommonBean, Long> bean2LongTuple2, Tuple2<CommonBean, Long> acc1) {
        CommonBean bean = bean2LongTuple2.f0;
        Long count = bean2LongTuple2.f1;
        Long acc = acc1.f1;

        return new Tuple2<>(bean, count + acc);
    }
}

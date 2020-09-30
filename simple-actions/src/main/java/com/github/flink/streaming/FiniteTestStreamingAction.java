package com.github.flink.streaming;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.FiniteTestSource;

import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2020/9/30 11:44 上午
 */
public class FiniteTestStreamingAction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple2<Integer, String>> exampleData = Lists.newArrayList(
                Tuple2.of(1, "a"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c"),
                Tuple2.of(4, "d")
        );

        env.addSource(new FiniteTestSource<>(exampleData))
                .returns(Types.TUPLE(Types.INT, Types.STRING)).print();

        env.execute();
    }
}

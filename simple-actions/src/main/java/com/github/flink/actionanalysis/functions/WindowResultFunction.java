package com.github.flink.actionanalysis.functions;

import com.github.flink.actionanalysis.model.ItemViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/27 9:03 PM
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {
    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) throws Exception {
        Tuple1<Long> key = (Tuple1<Long>)tuple;
        Long itemId = key.f0;
        Long count = input.iterator().next();
        out.collect(new ItemViewCount(itemId, window.getEnd(), count));
    }
}

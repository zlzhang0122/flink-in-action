package com.github.flink.networkanalysis.functions;

import com.github.flink.actionanalysis.model.ItemViewCount;
import com.github.flink.networkanalysis.model.UrlViewCount;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 窗口函数输出
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/27 9:03 PM
 */
public class WindowResultFunction implements WindowFunction<Long, UrlViewCount, Tuple, TimeWindow> {
    private static final Logger logger = LoggerFactory.getLogger(WindowResultFunction.class);

    @Override
    public void apply(Tuple tuple, TimeWindow window, Iterable<Long> input, Collector<UrlViewCount> out) throws Exception {
        Tuple1<String> key = (Tuple1<String>)tuple;
        String url = key.f0;
        Long count = input.iterator().next();

        logger.debug("apply:" + url + " " + window.getEnd() + " " + count);

        out.collect(new UrlViewCount(url, window.getEnd(), count));
    }
}

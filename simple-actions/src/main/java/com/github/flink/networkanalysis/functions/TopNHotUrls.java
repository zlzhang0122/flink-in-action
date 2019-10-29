package com.github.flink.networkanalysis.functions;

import com.github.flink.networkanalysis.model.UrlViewCount;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 最热被访问url统计
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/29 6:51 PM
 */
public class TopNHotUrls extends KeyedProcessFunction<Tuple, UrlViewCount, String> {
    private ListState<UrlViewCount> urlViewCountListState;

    private int topSize;

    public TopNHotUrls(int topSize) {
        this.topSize = topSize;
    }

    /**
     * 获得liststate
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ListStateDescriptor<UrlViewCount> listStateDescriptor = new ListStateDescriptor<UrlViewCount>("urlState-state", UrlViewCount.class);
        urlViewCountListState = getRuntimeContext().getListState(listStateDescriptor);
    }

    /**
     * 增加state,并注册定时器
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {
        urlViewCountListState.add(value);

        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<UrlViewCount> urlViewCountList = new ArrayList<>();
        for (UrlViewCount urlViewCount : urlViewCountListState.get()) {
            urlViewCountList.add(urlViewCount);
        }
        //提前清除状态
        urlViewCountListState.clear();
        urlViewCountList.sort(new Comparator<UrlViewCount>() {
            @Override
            public int compare(UrlViewCount o1, UrlViewCount o2) {
                if (o1.getCount() < o2.getCount()) {
                    return -1;
                } else {
                    return 1;
                }
            }
        });
        urlViewCountList = urlViewCountList.subList(0, topSize);
        StringBuilder res = new StringBuilder();
        res.append("========");
        res.append("时间:").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < urlViewCountList.size(); i++) {
            UrlViewCount item = urlViewCountList.get(i);

            res.append("No").append(i + 1).append(":");
            res.append(" URL=").append(item.getUrl());
            res.append(" 浏览量=").append(item.getCount()).append("\n");
        }
        res.append("========\n\n");
        Thread.sleep(1000);
        out.collect(res.toString());
    }
}

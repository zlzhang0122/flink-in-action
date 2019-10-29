package com.github.flink.actionanalysis.functions;

import com.github.flink.actionanalysis.model.ItemViewCount;
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
 * 计算热门商品TopN
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/28 6:10 PM
 */
public class TopNHotItems extends KeyedProcessFunction<Tuple, ItemViewCount, String> {
    private ListState<ItemViewCount> itemViewCountListState;

    private int topSize;

    public TopNHotItems(int topSize) {
        this.topSize = topSize;
    }

    /**
     * 注册state
     *
     * @param configuration
     * @throws Exception
     */
    @Override
    public void open(Configuration configuration) throws Exception{
        super.open(configuration);
        ListStateDescriptor<ItemViewCount> descriptor = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
        itemViewCountListState = getIterationRuntimeContext().getListState(descriptor);
    }

    /**
     * state存储,注册触发器
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        itemViewCountListState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    /**
     * 触发器逻辑
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> list = new ArrayList<>();
        for(ItemViewCount item : itemViewCountListState.get()){
            list.add(item);
        }

        itemViewCountListState.clear();
        list.sort(new Comparator<ItemViewCount>() {
            @Override
            public int compare(ItemViewCount o1, ItemViewCount o2) {
                if(o1.getCount() < o2.getCount()){
                    return -1;
                }else{
                    return 1;
                }
            }
        });
        list = list.subList(0, topSize);

        StringBuilder res = new StringBuilder();
        res.append("========");
        res.append("时间:").append(new Timestamp(timestamp - 1)).append("\n");

        for(int i = 0; i< list.size(); i++){
            ItemViewCount currItem = list.get(i);

            res.append("No").append(i+1).append(":");
            res.append(" 商品ID=").append(currItem.getItemId());
            res.append(" 浏览量=").append(currItem.getCount()).append("\n");
        }
        res.append("========\n\n");

        //休息1秒
        Thread.sleep(1000);
        out.collect(res.toString());
    }
}

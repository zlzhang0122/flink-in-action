package com.github.flink.streamjoin.functions;

import com.github.flink.streamjoin.model.StockSnapshot;
import com.github.flink.streamjoin.model.StockTransaction;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/3 10:05 AM
 */
public class UserCustomInnerJoinFunction implements CoGroupFunction<StockTransaction, StockSnapshot,
        Tuple6<String, String, String, Double, Double, String>> {
    @Override
    public void coGroup(Iterable<StockTransaction> first, Iterable<StockSnapshot> second, Collector<Tuple6<String, String, String, Double, Double, String>> out) throws Exception {
        if(first.iterator().hasNext() && second.iterator().hasNext()){
            while(first.iterator().hasNext()){
                StockTransaction stockTransaction = first.iterator().next();
                while(second.iterator().hasNext()){
                    StockSnapshot stockSnapshot = second.iterator().next();
                    out.collect(new Tuple6<>(stockTransaction.getTxCode(), stockTransaction.getTxTime(), stockSnapshot.getMdTime(), stockTransaction.getTxValue(), stockSnapshot.getMdValue(), "Inner Join"));
                }
            }
        }
    }
}

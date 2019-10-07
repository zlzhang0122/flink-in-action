package com.github.flink.function.map;

import com.github.flink.domain.LogEntity;
import com.github.flink.util.LogToEntity;
import com.github.flink.utils.client.HbaseClient;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午10:52
 */
public class UserHistoryMapFunction implements MapFunction<String, Void> {
    @Override
    public Void map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);

        System.out.println("s:" + s);
        if (null != log){
            HbaseClient.increamColumn("u_history", String.valueOf(log.getUserId()), "p", String.valueOf(log.getProductId()));
            HbaseClient.increamColumn("p_history", String.valueOf(log.getProductId()), "p", String.valueOf(log.getUserId()));
        }

        return null;
    }
}

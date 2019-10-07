package com.github.flink.function.map;

import com.github.flink.domain.LogEntity;
import com.github.flink.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/16 下午10:08
 */
public class TopProductMapFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);

        return log;
    }
}

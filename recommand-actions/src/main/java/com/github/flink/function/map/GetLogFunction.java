package com.github.flink.function.map;

import com.github.flink.domain.LogEntity;
import com.github.flink.util.LogToEntity;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午11:21
 */
public class GetLogFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String s) throws Exception {
        LogEntity entity = LogToEntity.getLog(s);

        return entity;
    }
}

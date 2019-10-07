package com.github.flink.function.map;

import com.github.flink.domain.LogEntity;
import com.github.flink.util.LogToEntity;
import com.github.flink.utils.client.HbaseClient;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/12 18:36
 */
public class LogMapFunction implements MapFunction<String, LogEntity> {
    @Override
    public LogEntity map(String value) throws Exception {
        LogEntity entity = LogToEntity.getLog(value);
        if (null != entity){
            String rowKey = entity.getUserId() + "_" + entity.getProductId()+ "_"+ entity.getTime();

            HbaseClient.putData("flink-recommand-log",rowKey,"log","userid",String.valueOf(entity.getUserId()));
            HbaseClient.putData("flink-recommand-log",rowKey,"log","productid",String.valueOf(entity.getProductId()));
            HbaseClient.putData("flink-recommand-log",rowKey,"log","time",entity.getTime().toString());
            HbaseClient.putData("flink-recommand-log",rowKey,"log","action",entity.getAction());
        }

        return entity;
    }
}

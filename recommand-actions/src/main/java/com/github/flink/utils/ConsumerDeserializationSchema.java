package com.github.flink.utils;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 17:43
 */
public class ConsumerDeserializationSchema<T> implements DeserializationSchema<T> {
    private Class<T> clazz;

    public ConsumerDeserializationSchema(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        //确保 new String(bytes) 是json 格式，如果不是，请自行解析
        return JSON.parseObject(new String(bytes), clazz);
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}

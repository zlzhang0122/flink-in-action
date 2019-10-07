package com.github.flink.rdbms.commons;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 18:13
 */
public class FlatMap implements MapFunction<JSONObject, CommonBean> {
    @Override
    public CommonBean map(JSONObject jsonObject) throws Exception {

        return new CommonBean(jsonObject.getLongValue("user_id"), jsonObject.getLongValue("item_id"),
                jsonObject.getLongValue("category_id"), jsonObject.getString("behavior"),
                jsonObject.getString("ts")
        );
    }
}

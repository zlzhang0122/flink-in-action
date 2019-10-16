package com.github.flink.function.map;

import com.github.flink.dao.ProductDao;
import com.github.flink.domain.LogEntity;
import com.github.flink.util.LogToEntity;
import com.github.flink.utils.client.HbaseClient;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;

/**
 * 用户画像任务
 *
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午11:33
 */
public class UserPortraitMapFunction implements MapFunction<String, Void> {
    @Override
    public Void map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);

        ProductDao productDao = new ProductDao();
        ResultSet rst = productDao.selectById(log.getProductId());
        if (rst != null){
            while (rst.next()){
                String userId = String.valueOf(log.getUserId());

                String country = rst.getString("country");
                HbaseClient.increamColumn("user", userId, "country", country);
                String color = rst.getString("color");
                HbaseClient.increamColumn("user", userId, "color", color);
                String style = rst.getString("style");
                HbaseClient.increamColumn("user", userId, "style", style);
            }
        }
        return null;
    }
}

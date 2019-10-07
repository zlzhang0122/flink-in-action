package com.github.flink.function.map;

import com.github.flink.dao.ProductDao;
import com.github.flink.domain.LogEntity;
import com.github.flink.util.AgeUtil;
import com.github.flink.util.LogToEntity;
import com.github.flink.utils.client.HbaseClient;
import com.github.flink.utils.client.MysqlClient;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.ResultSet;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/13 下午8:29
 */
public class ProductPortraitMapFunction implements MapFunction<String, String> {

    @Override
    public String map(String s) throws Exception {
        LogEntity log = LogToEntity.getLog(s);

        ProductDao productDao = new ProductDao();
        ResultSet rst = productDao.selectUserById(log.getUserId());
        if (rst != null){
            while (rst.next()){
                String productId = String.valueOf(log.getProductId());

                String sex = rst.getString("sex");
                HbaseClient.increamColumn("prod", productId, "sex", sex);
                String age = rst.getString("age");
                HbaseClient.increamColumn("prod", productId,"age", AgeUtil.getAgeType(age));
            }
        }
        return null;
    }
}

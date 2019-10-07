package com.github.flink.rdbms;

import com.github.flink.rdbms.commons.CommonBean;
import com.github.flink.utils.client.MysqlClient;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/4 18:29
 */
public class JdbcWriter extends RichSinkFunction<Tuple2<CommonBean, Long>> {
    private Connection connection;

    private PreparedStatement preparedStatement;

    private static final String sql = "insert into visit_log(user_id,item_id,category_id,behavior,ts,num) values (?,?,?,?,?,?)";

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        connection = MysqlClient.getConnection();
        preparedStatement = connection.prepareStatement(sql);

        super.open(parameters);
    }

    public void invoke(Tuple2<CommonBean, Long> value, Context context) throws Exception {
        try {
            CommonBean commonBean = value.f0;
            Long userId = commonBean.getUserId();
            Long itemId = commonBean.getItemId();
            Long categoryId = commonBean.getCategoryId();
            String behavior = commonBean.getBehavior();
            String ts = commonBean.getTs();
            Long num = value.f1;

            preparedStatement.setLong(1, userId);
            preparedStatement.setLong(2, itemId);
            preparedStatement.setLong(3, categoryId);
            preparedStatement.setString(4, behavior);
            preparedStatement.setString(5, ts);
            preparedStatement.setLong(6, num);

            preparedStatement.executeUpdate();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(preparedStatement != null){
            preparedStatement.close();
        }
        if(connection != null){
            connection.close();
        }
        super.close();
    }
}
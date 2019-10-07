package com.github.flink.dao;

import com.github.flink.utils.client.MysqlClient;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/13 下午9:00
 */
public class ProductDao {

    private Statement statement;

    public ProductDao(){
        statement = MysqlClient.getStmt();
    }

    public ProductDao(Statement statement){
        this.statement = statement;
    }

    /**
     * 根据Id筛选产品
     *
     * @param id
     * @return
     * @throws SQLException
     */
    public ResultSet selectById(int id) throws SQLException {
        String sql = String.format("select  * from product where product_id = %s",id);
        return statement.executeQuery(sql);
    }

    /**
     * 根据Id筛选用户
     *
     * @param id
     * @return
     * @throws SQLException
     */
    public ResultSet selectUserById(int id) throws SQLException{
        String sql = String.format("select  * from user where user_id = %s",id);
        return statement.executeQuery(sql);
    }

    public static void main(String[] args) throws SQLException {
        ProductDao productDao = new ProductDao();

        ResultSet resultSet = productDao.selectById(1);
        while (resultSet.next()) {
            System.out.println(resultSet.getString(2));
        }
    }
}

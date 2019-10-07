package com.github.flink.utils.client;

import com.github.flink.utils.PropertiesUtil;

import java.sql.*;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/13 下午8:22
 */
public class MysqlClient {

    private static String URL = PropertiesUtil.getStrValue("mysql.jdbc.url");
    private static String NAME = PropertiesUtil.getStrValue("mysql.jdbc.name");
    private static String PASS = PropertiesUtil.getStrValue("mysql.jdbc.passwd");

    private static Statement stmt;
    private static Connection connection;

    static {
        try {
            // 加载JDBC驱动
            Class.forName("com.mysql.cj.jdbc.Driver");
            // 获取数据库连接
            connection = DriverManager.getConnection(URL, NAME, PASS);
            //写入mysql数据库
            stmt = connection.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Statement getStmt() {
        return stmt;
    }

    public static Connection getConnection() {
        return connection;
    }
}

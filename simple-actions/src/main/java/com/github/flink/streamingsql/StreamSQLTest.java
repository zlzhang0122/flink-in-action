package com.github.flink.streamingsql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

/**
 * @Author: zlzhang0122
 * @Date: 2021/9/16 2:27 下午
 */
public class StreamSQLTest {
    public static void main(String[] args) throws Exception {


//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()...build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//// to use hive dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
//// to use default dialect
//        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        // set up execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        // convert DataStream to Table
        Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");

        // union the two tables
        Table result = tEnv.sqlQuery("SELECT user, count(*) as amount FROM " + tableA + " group by 1");

        tEnv.toRetractStream(result, OrderCount.class).print();

        env.execute();
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }

    public static class OrderCount {
        public Long user;
        public Long amount;

        public OrderCount() {
        }

        public OrderCount(Long user, Long amount) {
            this.user = user;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", amount=" + amount +
                    '}';
        }
    }
}

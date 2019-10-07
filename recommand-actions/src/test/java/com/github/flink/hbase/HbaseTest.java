package com.github.flink.hbase;

import com.github.flink.utils.client.HbaseClient;
import org.junit.Test;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/17 上午11:23
 */
public class HbaseTest {

    @Test
    public void run() throws Exception{
        HbaseClient.createTable("u_history", "p");
        HbaseClient.createTable("p_history", "p");
    }
}

package com.github.flink.random;

import org.junit.Test;

import java.util.Random;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/17 下午4:32
 */
public class RandomTest {

    @Test
    public void test(){

        Random userIdRandom = new Random(System.currentTimeMillis());
        int userIdIndex = userIdRandom.nextInt(3);
        System.out.println(userIdIndex);

        int productIdIndex = userIdRandom.nextInt(5);
        System.out.println(productIdIndex);

        int timeIndex = userIdRandom.nextInt(10);
        System.out.println(timeIndex);

        int actionIndex = userIdRandom.nextInt(3);
        System.out.println(actionIndex);
    }
}

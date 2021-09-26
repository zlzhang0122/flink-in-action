package com.github.flink.fakesource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.Serializable;
import java.util.Random;

/**
 * @Author: zlzhang0122
 * @Date: 2021/9/22 下午7:10
 */
public class FakeSource implements SourceFunction<String>, Serializable {

    private static Random random = new Random(System.currentTimeMillis());
    private volatile boolean isRunning = true;

    // sleep的毫秒数
    private long sleepMillis;

    public FakeSource() {
        new FakeSource(500l);
    }

    public FakeSource(long sleepMills) {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(fakeTrafficRecordString());
            Thread.sleep(sleepMillis);
        }
    }

    // 生成流量记录
    // 字段：用户id、城市id、上行流量、下行流量、发生时间
    public String fakeTrafficRecordString() {
        String accountId = "user_" + random.nextInt(5000);
        String cityId = "city_" + random.nextInt(50);
        String upTrafficStr = "up_" + random.nextInt(1000000);
        String downTrafficStr = "down_" + random.nextInt(10000);
        String eventTime = "time_" + System.currentTimeMillis();

        return accountId + " " + cityId + " " + upTrafficStr + " " + downTrafficStr + " " + eventTime;
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
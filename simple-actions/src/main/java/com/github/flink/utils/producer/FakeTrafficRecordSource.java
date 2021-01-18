package com.github.flink.utils.producer;

import com.github.flink.model.TrafficRecord;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Random;

/**
 * @Author: zlzhang0122
 * @Date: 2021/1/18 4:44 下午
 */
public class FakeTrafficRecordSource implements SourceFunction<TrafficRecord> {

    private static Random random = new Random();
    private volatile boolean isRunning = true;

    // sleep的毫秒数
    private long sleepMillis;

    public FakeTrafficRecordSource() {
        new FakeTrafficRecordSource(500l);
    }

    public FakeTrafficRecordSource(long sleepMills) {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<TrafficRecord> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(fakeTrafficRecordString());
            Thread.sleep(sleepMillis);
        }
    }

    // 生成流量记录
    // 字段：用户id、城市id、上行流量、下行流量、发生时间
    public TrafficRecord fakeTrafficRecordString() {
        int accountId = random.nextInt(500);
        int cityId = random.nextInt(5);
        double upTraffic = new BigDecimal(random.nextDouble()).setScale(4, RoundingMode.HALF_UP).doubleValue();
        double downTraffic = new BigDecimal(random.nextDouble()).setScale(4, RoundingMode.HALF_UP).doubleValue();
        long eventTime = System.currentTimeMillis();

        return new TrafficRecord(accountId, cityId, upTraffic, downTraffic, eventTime);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

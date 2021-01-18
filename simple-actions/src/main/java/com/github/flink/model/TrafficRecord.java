package com.github.flink.model;

/**
 * @Author: zlzhang0122
 * @Date: 2021/1/18 4:43 下午
 */
public class TrafficRecord {
    public int accountId;
    public int cityId;
    public double upTraffic;
    public double downTraffic;
    public long eventTime;

    public TrafficRecord() {
    }

    public TrafficRecord(int accountId, int cityId, double upTraffic, double downTraffic, long eventTime) {
        this.accountId = accountId;
        this.cityId = cityId;
        this.upTraffic = upTraffic;
        this.downTraffic = downTraffic;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return
                "" +
                        accountId
                        + "|" +
                        cityId
                        + "|" +
                        upTraffic
                        + "|" +
                        downTraffic
                        + "|" +
                        eventTime
                ;
    }
}

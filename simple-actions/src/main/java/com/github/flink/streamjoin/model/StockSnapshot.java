package com.github.flink.streamjoin.model;

/**
 * 股票快照entity
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/31 7:00 PM
 */
public class StockSnapshot {
    public StockSnapshot() {

    }

    public StockSnapshot(String mdTime, String mdCode, Double mdValue) {
        this.mdTime = mdTime;
        this.mdCode = mdCode;
        this.mdValue = mdValue;
    }

    private String mdTime;

    private String mdCode;

    private Double mdValue;

    public String getMdTime() {
        return mdTime;
    }

    public void setMdTime(String mdTime) {
        this.mdTime = mdTime;
    }

    public String getMdCode() {
        return mdCode;
    }

    public void setMdCode(String mdCode) {
        this.mdCode = mdCode;
    }

    public Double getMdValue() {
        return mdValue;
    }

    public void setMdValue(Double mdValue) {
        this.mdValue = mdValue;
    }

    @Override
    public String toString() {
        return "StockSnapshot{" +
                "mdTime='" + mdTime + '\'' +
                ", mdCode='" + mdCode + '\'' +
                ", mdValue=" + mdValue +
                '}';
    }
}

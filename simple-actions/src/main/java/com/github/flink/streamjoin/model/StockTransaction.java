package com.github.flink.streamjoin.model;

/**
 * 股票交易entity
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/31 6:58 PM
 */
public class StockTransaction {
    public StockTransaction() {

    }

    public StockTransaction(String txTime, String txCode, Double txValue) {
        this.txTime = txTime;
        this.txCode = txCode;
        this.txValue = txValue;
    }

    private String txTime;

    private String txCode;

    private Double txValue;

    public String getTxTime() {
        return txTime;
    }

    public void setTxTime(String txTime) {
        this.txTime = txTime;
    }

    public String getTxCode() {
        return txCode;
    }

    public void setTxCode(String txCode) {
        this.txCode = txCode;
    }

    public Double getTxValue() {
        return txValue;
    }

    public void setTxValue(Double txValue) {
        this.txValue = txValue;
    }

    @Override
    public String toString() {
        return "StockTransaction{" +
                "txTime='" + txTime + '\'' +
                ", txCode='" + txCode + '\'' +
                ", txValue=" + txValue +
                '}';
    }
}

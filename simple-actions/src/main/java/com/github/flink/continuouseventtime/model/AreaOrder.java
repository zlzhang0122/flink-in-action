package com.github.flink.continuouseventtime.model;

/**
 * 区域销售额
 *
 * @Author: zlzhang0122
 * @Date: 2019/11/22 1:31 PM
 */
public class AreaOrder {
    private String areaId;

    private Double amount;

    public String getAreaId() {
        return areaId;
    }

    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    @Override
    public String toString() {
        return "AreaOrder{" +
                "areaId='" + areaId + '\'' +
                ", amount=" + amount +
                '}';
    }
}

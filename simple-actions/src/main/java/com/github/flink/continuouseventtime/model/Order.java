package com.github.flink.continuouseventtime.model;

/**
 * 订单
 *
 * @Author: zlzhang0122
 * @Date: 2019/11/22 1:49 PM
 */
public class Order {
    private String orderId;

    private Long orderTime;

    private String gdsId;

    private Double amount;

    private String areaId;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public Long getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(Long orderTime) {
        this.orderTime = orderTime;
    }

    public String getGdsId() {
        return gdsId;
    }

    public void setGdsId(String gdsId) {
        this.gdsId = gdsId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getAreaId() {
        return areaId;
    }

    public void setAreaId(String areaId) {
        this.areaId = areaId;
    }

    @Override
    public String toString() {
        return "Order{" +
                "orderId='" + orderId + '\'' +
                ", orderTime=" + orderTime +
                ", gdsId='" + gdsId + '\'' +
                ", amount=" + amount +
                ", areaId='" + areaId + '\'' +
                '}';
    }
}

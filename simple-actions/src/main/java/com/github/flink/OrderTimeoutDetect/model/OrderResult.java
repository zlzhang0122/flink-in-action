package com.github.flink.OrderTimeoutDetect.model;

/**
 * 订单结果
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 7:27 PM
 */
public class OrderResult {
    public OrderResult() {
    }

    public OrderResult(Long orderId, String eventType) {
        this.orderId = orderId;
        this.eventType = eventType;
    }

    private Long orderId;

    private String eventType;

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                '}';
    }
}

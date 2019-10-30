package com.github.flink.OrderTimeoutDetect.model;

/**
 * 订单事件
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 7:25 PM
 */
public class OrderEvent {

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, Long eventTime) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    private Long orderId;

    private String eventType;

    private Long eventTime;

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

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

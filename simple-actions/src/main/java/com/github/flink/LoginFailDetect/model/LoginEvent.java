package com.github.flink.LoginFailDetect.model;

import java.io.Serializable;

/**
 * 登录事件
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/30 6:12 PM
 */
public class LoginEvent implements Serializable {
    public LoginEvent() {
    }

    public LoginEvent(Long userId, String ip, String eventType, Long eventTime) {
        this.userId = userId;
        this.ip = ip;
        this.eventType = eventType;
        this.eventTime = eventTime;
    }

    private Long userId;

    private String ip;

    private String eventType;

    private Long eventTime;

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
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
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTime=" + eventTime +
                '}';
    }
}

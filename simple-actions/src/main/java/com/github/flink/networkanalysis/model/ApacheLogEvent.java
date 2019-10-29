package com.github.flink.networkanalysis.model;

/**
 * apache日志事件
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/29 6:06 PM
 */
public class ApacheLogEvent {

    public ApacheLogEvent() {
    }

    public ApacheLogEvent(String ip, String userId, Long eventTime, String method, String url) {
        this.ip = ip;
        this.userId = userId;
        this.eventTime = eventTime;
        this.method = method;
        this.url = url;
    }

    private String ip;

    private String userId;

    private Long eventTime;

    private String method;

    private String url;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getEventTime() {
        return eventTime;
    }

    public void setEventTime(Long eventTime) {
        this.eventTime = eventTime;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return "ApacheLogEvent{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", eventTime=" + eventTime +
                ", method='" + method + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}

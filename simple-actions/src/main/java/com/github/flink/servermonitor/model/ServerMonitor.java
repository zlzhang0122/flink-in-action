package com.github.flink.servermonitor.model;

import java.util.Date;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/11 4:13 PM
 */
public class ServerMonitor {

    public ServerMonitor() {

    }

    public ServerMonitor(String serverId, String flag, Date date) {
        this.serverId = serverId;
        this.flag = flag;
        this.date = date;
    }

    private String serverId;

    private String flag;

    private Date date;

    public String getServerId() {
        return serverId;
    }

    public void setServerId(String serverId) {
        this.serverId = serverId;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "ServerMonitor{" +
                "serverId='" + serverId + '\'' +
                ", flag='" + flag + '\'' +
                ", date=" + date +
                '}';
    }
}

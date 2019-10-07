package com.github.flink.domain;

import java.io.Serializable;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/15 下午11:29
 */
public class Action implements Serializable
{

    private String type;
    private String time;

    public Action(String type, String time) {
        this.type = type;
        this.time = time;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }
}
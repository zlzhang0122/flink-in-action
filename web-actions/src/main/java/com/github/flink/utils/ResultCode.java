package com.github.flink.utils;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 6:11 PM
 */
public enum  ResultCode {
    SUCCESS(0, "成功"),
    UNKNOWN_EXCEPTION(-1000, "未知异常"),
    BIZ_EXCEPTION(-1001, "业务异常"),
    PARAM_INVALID_EXCEPTION(-1002, "参数异常");

    private int code;

    private String msg;

    ResultCode(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}

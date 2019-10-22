package com.github.flink.utils;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/22 6:17 PM
 */
public class ResultUtil {
    public static Result success(){
        return new Result<>(ResultCode.SUCCESS, null);
    }

    public static Result success(Object data){
        return new Result<>(ResultCode.SUCCESS, data);
    }

    public static Result fail(ResultCode resultCode){
        return new Result(resultCode);
    }

    public static Result fail(ResultCode resultCode, String msg){
        Result<Object> result = new Result<>(resultCode);
        result.setMsg(msg);
        return result;
    }
}

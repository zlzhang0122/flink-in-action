package com.github.flink.util;

import com.github.flink.domain.LogEntity;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/12 18:37
 */
public class LogToEntity {
    public static LogEntity getLog(String s){
        System.out.println(s);

        String[] values = s.split(",");
        if (values.length < 2) {
            System.out.println("Message is not correct");
            return null;
        }

        LogEntity log = new LogEntity();

        log.setUserId(Integer.parseInt(values[0]));
        log.setProductId(Integer.parseInt(values[1]));

        if(values[2] == null){
            log.setTime(null);
        }else{
            log.setTime(Long.parseLong(values[2]));
        }

        log.setAction(values[3]);

        return log;
    }
}

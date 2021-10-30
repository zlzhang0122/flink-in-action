package com.github.flink.utils;

import org.testcontainers.shaded.org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * @Author: zlzhang0122
 * @Date: 2021/10/29 下午4:17
 */
public class YamlUtils {
    public static void main(String[] args){

        try {
            InputStream input = new FileInputStream("/Users/zhangjiao/flink-conf.yml");
            Yaml yaml = new Yaml();
            Map<String, Object> object = (Map<String, Object>) yaml.load(input);
            for(Map.Entry<String, Object> entry : object.entrySet()){
                System.out.println(entry.getKey() + " " + entry.getValue());
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}

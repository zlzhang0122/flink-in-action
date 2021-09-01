package com.github.flink;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;

/**
 * @Author: zlzhang0122
 * @Date: 2021/8/31 4:29 下午
 */
public class JsonTest {
    public static void main(String[] args) throws Exception{
        File file = new File("/Users/zhangjiao/test/a.json");
        JsonFactory jsonFactory = new JsonFactory();
        JsonParser jsonParser = jsonFactory.createParser(file);
        JsonNode arrNode = (JsonNode) new ObjectMapper().readTree(jsonParser).get("taskmanagers");
        if (arrNode.isArray()){
            for(JsonNode jsonNode : arrNode){
                System.out.println(jsonNode.get("id").asText() + ":");
            }
        }
    }
}

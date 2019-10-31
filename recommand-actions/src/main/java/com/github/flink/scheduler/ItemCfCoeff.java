package com.github.flink.scheduler;

import com.github.flink.utils.client.HbaseClient;

import java.util.List;
import java.util.Map;

/**
 * 基于协同过滤的产品相关度计算
 * * 策略1 ：协同过滤
 *      *           abs( i ∩ j)
 *      *      w = ——————————————
 *      *           sqrt(i || j)
 *
 * @Author: zlzhang0122
 * @Date: 2019/10/17 8:15 PM
 */
public class ItemCfCoeff {
    public void getSingleItemCfCoeff(String id, List<String> others) throws Exception {
        for(String other : others){
            if(id.equalsIgnoreCase(other)){
                continue;
            }

            Double score = twoItemCfCoeff(id, other);
            HbaseClient.putData("px", id, "p", other, score.toString());
        }
    }

    public Double twoItemCfCoeff(String id, String other) throws Exception {
        List<Map.Entry> p1 = HbaseClient.getRow("p_history", id);
        List<Map.Entry> p2 = HbaseClient.getRow("p_history", other);

        int n = p1.size();
        int m = p2.size();
        int sum = 0;
        Double total = Math.sqrt(n * m);
        for(Map.Entry entry : p1){
            String key = (String)entry.getKey();
            for(Map.Entry p : p2){
                if(key.equals(p.getKey())){
                    sum++;
                }
            }
        }

        if(total == 0){
            return 0.0;
        }

        return sum / total;
    }
}

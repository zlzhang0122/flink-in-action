package com.github.flink.scheduler;

import com.github.flink.utils.client.HbaseClient;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/17 9:15 PM
 */
public class SchedulerJob {
    static ExecutorService executorService = Executors.newFixedThreadPool(10);

    public static void main(String[] args){
        Timer qTimer = new Timer();
        qTimer.scheduleAtFixedRate(new RefreshTask(), 0, 15 * 1000);
    }

    private static class RefreshTask extends TimerTask {

        @Override
        public void run() {
            System.out.println(new Date() + " 开始执行任务!");

            List<String> allProductId = new ArrayList<>();
            try{
                allProductId = HbaseClient.getAllKey("p_history");
            }catch (Exception e){
                System.err.println("获取历史产品id异常:" + e.getMessage());
                e.printStackTrace();
                return;
            }

            for(String s : allProductId){
                executorService.execute(new Task(s, allProductId));
            }
        }
    }

    private static class Task implements Runnable {
        private String id;
        private List<String> others;

        public Task(String id, List<String> others){
            this.id = id;
            this.others = others;
        }

        ItemCfCoeff itemCfCoeff = new ItemCfCoeff();
        ProductCoeff productCoeff = new ProductCoeff();

        @Override
        public void run() {
            try{
                itemCfCoeff.getSingleItemCfCoeff(id, others);
                productCoeff.getSingleProductCoeff(id, others);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}

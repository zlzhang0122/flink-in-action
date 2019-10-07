package com.github.flink.dataproduce;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * @Author: zlzhang0122
 * @Date: 2019/9/16 下午10:28
 */
public class KafkaDataSource implements SourceFunction<String> {

    private boolean isRunning = true;

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {

//        books.add("1,123,1563799391,2");
//        books.add("1,123,1563799393,2");
//        books.add("1,124,1563799394,2");
//        books.add("1,122,1563799398,2");
//        books.add("1,122,1563799412,1");
//        books.add("1,122,1563799418,1");
//        books.add("1,112,1563799419,1");
//        books.add("1,112,1563799420,1");
//        books.add("1,112,1563799428,1");

        while (isRunning){

            StringBuffer stringBuffer = new StringBuffer();
            List<String> userIds = new ArrayList<>();
            userIds.add("1");
            userIds.add("2");
            userIds.add("3");

            Random random = new Random(System.currentTimeMillis());
            int userIdIndex = random.nextInt(3);

            String userId = userIds.get(userIdIndex);

            stringBuffer.append(userId + ",");

            List<String> productIds = new ArrayList<>();
            productIds.add("112");
            productIds.add("118");
            productIds.add("122");
            productIds.add("123");
            productIds.add("124");

            int productIdIndex = random.nextInt(5);
            String productId = productIds.get(productIdIndex);

            stringBuffer.append(productId + ",");

            int time = random.nextInt(10);
            Thread.sleep(time);
            stringBuffer.append(System.currentTimeMillis() + ",");

            List<String> actionIds = new ArrayList<>();
            actionIds.add("1");
            actionIds.add("2");

            int actionIdIndex = random.nextInt(2);
            String actionId = actionIds.get(actionIdIndex);

            stringBuffer.append(actionId);
            String s = stringBuffer.toString();
            System.out.println(s);

            sourceContext.collect(s);

            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

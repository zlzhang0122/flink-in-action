package com.github.flink.realtimeuv;

//import akka.remote.serialization.ProtobufSerializer;
import com.github.flink.utils.PropertiesUtil;
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * 实时UV计算
 *
 * @Author: zlzhang0122
 * @Date: 2020/9/16 4:18 下午
 */
public class RealtimeUV {
    public static void main(String[] args){

        Properties sourceTopic = PropertiesUtil.getKafkaProperties("uv.group");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.getConfig().registerTypeWithKryoSerializer(MobilePage.class,
//                ProtobufSerializer.class);



//        String topic = config.get("source.kafka.topic");
//        String groupId = config.get("source.group.id");
//        String sourceBootStrapServers = config.get("source.bootstrap.servers");
//        String hbaseTable = config.get("hbase.table.name");
//        String hbaseZkQuorum = config.get("hbase.zk.quorum");
//        String hbaseZkParent = config.get("hbase.zk.parent");
//        int checkPointPeriod = Integer.parseInt(config.get("checkpoint.period"));
//        int checkPointTimeout = Integer.parseInt(config.get("checkpoint.timeout"));


    }
}

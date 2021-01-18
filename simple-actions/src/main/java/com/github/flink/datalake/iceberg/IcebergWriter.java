package com.github.flink.datalake.iceberg;

import com.github.flink.utils.FlinkKafkaManager;
import com.github.flink.utils.PropertiesUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.util.Properties;
import java.util.Random;

/**
 * 写入Iceberg的Table中
 *
 * @Author: zlzhang0122
 * @Date: 2021/1/18 9:59 上午
 */
public class IcebergWriter {
    private static final Log logger = LogFactory.getLog(IcebergWriter.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(5000L);
        env.setParallelism(1);

        // Checkpoint配置.
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointInterval(5 * 60* 1000);
        config.setMinPauseBetweenCheckpoints(5 * 60 * 1000);
        config.setCheckpointTimeout(10 * 60* 1000);

        // 如果可以接受作业失败重启时发生数据重复的话，可以设置为AT_LEAST_ONCE，这样会加快Checkpoint速度。
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        config.enableUnalignedCheckpoints(true);
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.setStateBackend(new MemoryStateBackend());
        env.setStateBackend(new RocksDBStateBackend("hdfs://localhost:9000/user/zhangjiao/iceberg/ck", true));

        Properties properties = PropertiesUtil.getKafkaProperties("iceberg-writer");
        FlinkKafkaManager manager = new FlinkKafkaManager("iceberg-writer", properties);
        FlinkKafkaConsumer<String> consumer = manager.buildStringConsumer();
        //从最近的消息开始处理
        consumer.setStartFromLatest();

        DataStreamSource<String> dataStream = env.addSource(consumer);
        // 指定UID可以更好地兼容版本升级.
        dataStream.uid("kafka-source");

        DataStream<RowData> input = dataStream.map((v) -> {
            Random random = new Random(System.currentTimeMillis());
            GenericRowData row = new GenericRowData(2);
            row.setField(0, random.nextLong());
            row.setField(1, StringData.fromString(v));
            return row;
        });

        Configuration configuration = new Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://localhost:9000/user/zhangjiao/path/iceberg/sample");

        FlinkSink.forRowData(input)
                .tableLoader(tableLoader)
                .overwrite(false)
                .build();

        env.execute("iceberg");
    }
}

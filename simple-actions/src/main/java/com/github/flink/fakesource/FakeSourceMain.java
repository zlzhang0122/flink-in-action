package com.github.flink.fakesource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: zlzhang0122
 * @Date: 2021/9/22 下午7:22
 */
public class FakeSourceMain {
    private static final Log logger = LogFactory.getLog(FakeSourceMain.class);

    public static void main(String[] args2) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checkpoint配置.
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointInterval(60* 1000);
        config.setMinPauseBetweenCheckpoints(10 * 1000);
        config.setCheckpointTimeout(60* 1000);
        config.setMaxConcurrentCheckpoints(1);

        // 如果可以接受作业失败重启时发生数据重复的话，可以设置为AT_LEAST_ONCE，这样会加快Checkpoint速度。
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        config.enableUnalignedCheckpoints(true);
        config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage(checkpointDir);
        env.setStateBackend(new RocksDBStateBackend("file:///Users/zhangjiao/work/tmp/ck1", true));

        DataStreamSource<String> talosSource = env.addSource(new FakeSource());
        // 指定UID可以更好地兼容版本升级.
        talosSource.uid("fake-source");

//        // 配置DataStream的转换操作.
        DataStream<Tuple2<String, Integer>> countStream = talosSource.flatMap(new Tokenizer())
                .uid("line-split")
                .keyBy(0)
                .countWindow(4, 2)
                .sum(1)
                .keyBy(0)
                .flatMap(new MyRichFunction())
                .uid("word-count");

        // 配置Sink
        countStream.addSink(new PrintSinkFunction<>());

        env.execute("flink-fakesource-test");
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    private static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws RuntimeException {
            // normalize and split the line
            try{
                String[] tokens = value.toLowerCase().split("\\W+");

                // emit the pairs
                for (String token : tokens) {
                    if (token.length() > 0) {
                        out.collect(new Tuple2<>(token, 1));
                    }
                }
            }catch (Exception e){
                logger.error("error in flatMap", e);
                e.printStackTrace();
            }
        }
    }

    private static final class MyRichFunction extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> implements CheckpointListener {
        private transient ValueState<String> cacheValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("CacheValue", String.class);
            cacheValue = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void flatMap(Tuple2<String, Integer> in, Collector<Tuple2<String, Integer>> out) {
            try{
                cacheValue.update(in.f0);
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public void notifyCheckpointComplete(long l) throws Exception {

        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) throws Exception {

        }
    }
}
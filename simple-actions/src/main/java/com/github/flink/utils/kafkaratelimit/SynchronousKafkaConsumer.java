package com.github.flink.utils.kafkaratelimit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.logging.Logger;

/**
 * @Author: zlzhang0122
 * @Date: 2021/8/8 5:06 下午
 */
public class SynchronousKafkaConsumer<T> extends FlinkKafkaConsumer<T> {
    protected static final Logger logger = (Logger) LoggerFactory.getLogger(SynchronousKafkaConsumer.class);

    private final double topicRateLimit;
    private transient RateLimiter subtaskRateLimiter;

    SynchronousKafkaConsumer(String topic,
                             TypeInformationSerializationSchema<T> schema,
                             TypeSerializer<T> typeSerializer,
                             Properties props,
                             double topicRateLimit) {
        super(topic, schema, props);
        Preconditions.checkArgument(props.getProperty("producer parallelism") != null, "Missing producer parallelism for Kafka Shuffle");
        Preconditions.checkArgument(topicRateLimit > 0, "topicRateLimit should be greater than 0");
        this.topicRateLimit = topicRateLimit;
    }

    public void open(Configuration configuration) throws Exception {
        Preconditions.checkArgument(topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks() > 0.1,
                "subtask ratelimit should be greater than 0.1 QPS");
        subtaskRateLimiter = RateLimiter.create(topicRateLimit / getRuntimeContext().getNumberOfParallelSubtasks());
        super.open(configuration);
    }

    @Override
    protected AbstractFetcher<T, ?> createFetcher(
            SourceContext<T> sourceContext,
            Map<KafkaTopicPartition, Long> partitionsWithOffsets,
            SerializedValue<WatermarkStrategy<T>> watermarkStrategy,
            StreamingRuntimeContext runtimeContext,
            OffsetCommitMode offsetCommitMode,
            MetricGroup consumerMetricGroup,
            boolean useMetrics) throws Exception {

        return new KafkaFetcher<T>(
                sourceContext,
                partitionsWithOffsets,
                watermarkStrategy,
                runtimeContext.getProcessingTimeService(),
                runtimeContext.getExecutionConfig().getAutoWatermarkInterval(),
                runtimeContext.getUserCodeClassLoader(),
                runtimeContext.getTaskNameWithSubtasks(),
                deserializer,
                properties,
                pollTimeout,
                runtimeContext.getMetricGroup(),
                consumerMetricGroup,
                useMetrics) {
            @Override
            protected void emitRecordsWithTimestamps(Queue<T> records,
                                                   KafkaTopicPartitionState<T, TopicPartition> partitionState,
                                                   long offset,
                                                   long timestamp) {
                subtaskRateLimiter.acquire();
                if (records == null || records.size() == 0) {
                    consumerMetricGroup.counter("invalidRecord").inc();
                }
                super.emitRecordsWithTimestamps(records, partitionState, offset, timestamp);
            }
        };
    }
}

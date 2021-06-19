package com.github.flink.connector.redis;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisDataType;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import static com.github.flink.connector.redis.RedisDynamicTableFactory.COMMAND;

/**
 * @Author: zlzhang0122
 * @Date: 2021/6/19 2:09 下午
 */
public class RedisDynamicTableSink implements DynamicTableSink {
    private final ReadableConfig options;
    private final TableSchema schema;

    public RedisDynamicTableSink(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.UPDATE_BEFORE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        Preconditions.checkNotNull(options, "No options supplied");

        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
//        FlinkJedisConfigBase jedisConfig = Util.getFlinkJedisConfig(options);
        Preconditions.checkNotNull(jedisConfig, "No Jedis config supplied");

        RedisCommand command = RedisCommand.valueOf(options.get(COMMAND).toUpperCase());

        int fieldCount = schema.getFieldCount();
        if (fieldCount != (needAdditionalKey(command) ? 3 : 2)) {
            throw new ValidationException("Redis sink only supports 2 or 3 columns");
        }

        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }

        RedisMapper<RowData> mapper = new RedisRowDataMapper(options, command);
        RedisSink<RowData> redisSink = new RedisSink<>(jedisConfig, mapper);
        return SinkFunctionProvider.of(redisSink);
    }

    private static boolean needAdditionalKey(RedisCommand command) {
        return command.getRedisDataType() == RedisDataType.HASH || command.getRedisDataType() == RedisDataType.SORTED_SET;
    }

    public static final class RedisRowDataMapper implements RedisMapper<RowData> {
        private static final long serialVersionUID = 1L;

        private final ReadableConfig options;
        private final RedisCommand command;

        public RedisRowDataMapper(ReadableConfig options, RedisCommand command) {
            this.options = options;
            this.command = command;
        }

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(command, "default-additional-key");
        }

        @Override
        public String getKeyFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 1 : 0).toString();
        }

        @Override
        public String getValueFromData(RowData data) {
            return data.getString(needAdditionalKey(command) ? 2 : 1).toString();
        }
    }

    @Override
    public DynamicTableSink copy() {
        return new RedisDynamicTableSink(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Sink";
    }
}

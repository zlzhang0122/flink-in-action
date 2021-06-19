package com.github.flink.connector.redis;

import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainer;
import org.apache.flink.streaming.connectors.redis.common.container.RedisCommandsContainerBuilder;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.util.Preconditions;

import java.util.concurrent.TimeUnit;

import static com.github.flink.connector.redis.RedisDynamicTableFactory.*;

/**
 * @Author: zlzhang0122
 * @Date: 2021/6/19 2:23 下午
 */
public class RedisRowDataLookupFunction extends TableFunction<RowData> {
    private static final long serialVersionUID = 1L;

    private final ReadableConfig options;
    private final String command;
    private final String additionalKey;
    private final int cacheMaxRows;
    private final int cacheTtlSec;

    private RedisCommandsContainer commandsContainer;
    private transient Cache<RowData, RowData> cache;

    public RedisRowDataLookupFunction(ReadableConfig options) {
        Preconditions.checkNotNull(options, "No options supplied");
        this.options = options;

        command = options.get(COMMAND).toUpperCase();
        Preconditions.checkArgument(command.equals("GET") || command.equals("HGET"), "Redis table source only supports GET and HGET commands");

        additionalKey = options.get(LOOKUP_ADDITIONAL_KEY);
        cacheMaxRows = options.get(LOOKUP_CACHE_MAX_ROWS);
        cacheTtlSec = options.get(LOOKUP_CACHE_TTL_SEC);
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);

        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
//        FlinkJedisConfigBase jedisConfig = Util.getFlinkJedisConfig(options);
        commandsContainer = RedisCommandsContainerBuilder.build(jedisConfig);
        commandsContainer.open();

        if (cacheMaxRows > 0 && cacheTtlSec > 0) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(cacheTtlSec, TimeUnit.SECONDS)
                    .maximumSize(cacheMaxRows)
                    .build();
        }
    }

    @Override
    public void close() throws Exception {
        if (cache != null) {
            cache.invalidateAll();
        }
        if (commandsContainer != null) {
            commandsContainer.close();
        }
        super.close();
    }

    public void eval(Object obj) {
        RowData lookupKey = GenericRowData.of(obj);
        if (cache != null) {
            RowData cachedRow = cache.getIfPresent(lookupKey);
            if (cachedRow != null) {
                collect(cachedRow);
                return;
            }
        }

        StringData key = lookupKey.getString(0);
        String value = "test";
//        String value = command.equals("GET") ? commandsContainer..get(key.toString()) : commandsContainer.hget(additionalKey, key.toString());
        RowData result = GenericRowData.of(key, StringData.fromString(value));

        cache.put(lookupKey, result);
        collect(result);
    }
}

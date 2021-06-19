package com.github.flink.connector.redis;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.util.Preconditions;

/**
 * @Author: zlzhang0122
 * @Date: 2021/6/19 2:19 下午
 */
public class RedisDynamicTableSource implements LookupTableSource {
    private final ReadableConfig options;
    private final TableSchema schema;

    public RedisDynamicTableSource(ReadableConfig options, TableSchema schema) {
        this.options = options;
        this.schema = schema;
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        Preconditions.checkArgument(context.getKeys().length == 1 && context.getKeys()[0].length == 1, "Redis source only supports lookup by single key");

        int fieldCount = schema.getFieldCount();
        if (fieldCount != 2) {
            throw new ValidationException("Redis source only supports 2 columns");
        }

        DataType[] dataTypes = schema.getFieldDataTypes();
        for (int i = 0; i < fieldCount; i++) {
            if (!dataTypes[i].getLogicalType().getTypeRoot().equals(LogicalTypeRoot.VARCHAR)) {
                throw new ValidationException("Redis connector only supports STRING type");
            }
        }

        return TableFunctionProvider.of(new RedisRowDataLookupFunction(options));
    }

    @Override
    public DynamicTableSource copy() {
        return new RedisDynamicTableSource(options, schema);
    }

    @Override
    public String asSummaryString() {
        return "Redis Dynamic Table Source";
    }
}

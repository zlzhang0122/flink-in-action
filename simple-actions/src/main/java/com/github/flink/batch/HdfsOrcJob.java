package com.github.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/15 6:03 PM
 */
public class HdfsOrcJob {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);

        OrcTableSource orcTs = OrcTableSource.builder().path("hdfs://127.0.0.1:9000/user/zhangjiao/events.txt")
                .forOrcSchema("struct<id:int,name:string,count:int>").build();
        tableEnvironment.registerTableSource("OrcTable", orcTs);
        Table result = tableEnvironment.sqlQuery("select * from OrcTable");

        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(result, Row.class);

        rowDataSet.print();
        tableEnvironment.execute("aa");
    }
}

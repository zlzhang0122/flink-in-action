package com.github.flink.batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.orc.OrcTableSource;
import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.orc.TypeDescription;

/**
 * @Author: zlzhang0122
 * @Date: 2019/10/15 6:03 PM
 */
public class HdfsOrcJob {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        BatchTableEnvironment tableEnvironment = BatchTableEnvironment.create(env);

        TypeDescription schema = TypeDescription.fromString("struct<id:int,name:int,count:int>");
//        OrcTableSource orcTs = OrcTableSource.builder().path("hdfs://localhost:9000/user/zhangjiao/orc/test.orc")
//                .forOrcSchema(schema).build();
//        tableEnvironment.registerTable("OrcTable", orcTs);
//        Table result = tableEnvironment.sqlQuery("select * from OrcTable");

//        DataSet<Row> rowDataSet = tableEnvironment.toDataSet(result, Row.class);

//        rowDataSet.print();
//        tableEnvironment.execute("aa");
    }
}

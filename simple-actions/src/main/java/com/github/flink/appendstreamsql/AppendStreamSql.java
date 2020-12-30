package com.github.flink.appendstreamsql;

import com.github.flink.appendstreamsql.model.Person;
import com.sun.rowset.internal.BaseRow;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @Author: zlzhang0122
 * @Date: 2019/11/5 7:11 PM
 */
public class AppendStreamSql {
    private static final String INPUT_NAME = "person.txt";

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        List<String> logs = new ArrayList<>();
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(INPUT_NAME);
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

            String str = "";
            while(StringUtils.isNoneBlank(str = bufferedReader.readLine())){
                logs.add(str);
            }
        } catch (IOException e) {
            System.err.println("文件加载失败!");
            e.printStackTrace();
        }

        DataStream<String> inDataStream = env.fromCollection(logs);
        DataStream<Person> source = inDataStream.map(new MapFunction<String, Person>() {
            @Override
            public Person map(String value) throws Exception {
                String[] arr = value.split("\\s+");
                return new Person(arr[0], arr[1]);
            }
        });

        Table table = tableEnvironment.fromDataStream(source);
        tableEnvironment.registerTable("person", table);
        Table res = tableEnvironment.sqlQuery("select * from person").select("name");
        DataStream<String> appendStream = tableEnvironment.toAppendStream(res, BaseRow.class).map(new MapFunction<BaseRow, String>() {
            @Override
            public String map(BaseRow value) throws Exception {
                return value.toString();
            }
        });
        appendStream.print();

        env.execute("Flink Append Sql");
    }
}

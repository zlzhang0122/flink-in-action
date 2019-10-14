#!/usr/bin/env bash

##run command:
# flink可执行脚本 run -d -p 并行度 jar包 -w 资源文件位置 -f 资源文件名称
#需要将配置文件中改变类解析顺序为classloader.resolve-order: parent-first
#需要在lib目录中增加:flink-sql-connector-kafka_2.11-1.9.0.jar
#flink-json-1.9.0-sql-jar.jar
#flink-jdbc_2.11-1.9.0.jar
#mysql-connector-java-5.1.48.jar
#flink-json-1.9.0.jar五个文件
#./bin/flink run -d -p 4 simple-actions-1.0-SNAPSHOT-jar-with-dependencies.jar -w src/main/resources/ -f study.sql
#for example:
./bin/flink run -d -p 4 ~/workspace/github/flink-in-action/simple-actions/target/simple-actions-1.0-SNAPSHOT-jar-with-dependencies.jar -w ~/workspace/github/flink-in-action/simple-actions/src/main/resources/ -f study.sql
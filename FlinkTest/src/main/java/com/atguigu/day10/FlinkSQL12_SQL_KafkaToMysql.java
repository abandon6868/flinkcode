package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQL12_SQL_KafkaToMysql {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2 从kafka 读取数据 注册sourceTable
        tableEnv.executeSql(
                "create table source_sensor(id string,ts bigint,vc int) with("
                        + "'connector'='kafka',"
                        + "'topic'='topic_source',"
                        + "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                        + "'properties.group.id'='flink_stu',"
                        + "'scan.startup.mode'='latest-offset',"
                        + "'format'='csv'"
                        +")");

        // 3.注册SinkTable：MySQL,表并不会自动创建
        tableEnv.executeSql(
                "create table source_sink(id string,ts bigint,vc int) with("
                + "'connector' = 'jdbc',"
                + "'url' = 'jdbc:mysql://hadoop102:3306/test',"
                + "'table-name' = 'sensor_test',"
                + "'username'='root',"
                + "'password'='admin'"
                +")");

        tableEnv.executeSql("insert into source_sink select * from source_sensor");

    }
}

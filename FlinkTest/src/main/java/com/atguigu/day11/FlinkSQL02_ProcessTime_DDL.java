package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/***
 * 从ddl 方式中 指定处理时间
 */
public class FlinkSQL02_ProcessTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用DDL 的方式指定处理时间字段
        tableEnv.executeSql(
        "create table source_sensor(id string,ts bigint,vc int,pt as PROCTIME()) with("
                + "'connector'='kafka',"
                + "'topic'='topic_source',"
                + "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'properties.group.id'='flink_stu',"
                + "'scan.startup.mode'='latest-offset',"
                + "'format'='csv'"
                +")");

        // 将DDL 创建的表转换为动态表
        Table table = tableEnv.from("source_sensor");
        table.printSchema();

    }
}

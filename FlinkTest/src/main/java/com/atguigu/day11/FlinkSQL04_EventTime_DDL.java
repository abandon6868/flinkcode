package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL04_EventTime_DDL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.使用DDL的方式指定事件时间字段
        tableEnv.executeSql(
        "create table source_sensor(id string," +
                "ts bigint," +
                "vc int," +
                "rt as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:ss:dd'))," +  // 指定事件时间，这个rt格式需要为 yyyy-MM-dd HH:ss:dd
                "WATERMARK FOR rt AS rt - INTERVAL '5' SECOND" +  // 如果需要延迟关窗，需设置这个
                ") with("
                + "'connector'='kafka',"
                + "'topic'='topic_source',"
                + "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092',"
                + "'properties.group.id'='flink_stu',"
                + "'scan.startup.mode'='latest-offset',"
                + "'format'='csv'"
                +")");

        //
        Table table = tableEnv.from("source_sensor");
        table.printSchema();

    }
}

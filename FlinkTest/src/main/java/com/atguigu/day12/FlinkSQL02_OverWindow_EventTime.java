package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_OverWindow_EventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useAnyPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        }).assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(
                        Duration.ofSeconds(2)).withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                }));

        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("rt").rowtime());

        // 基于事件时间的Over窗口
        Table result = table.window(Over.partitionBy($("id")).orderBy($("rt")).as("ct"))
                .select($("id"), $("id").count().over($("ct")));

        result.execute().print();

        env.execute();
    }
}

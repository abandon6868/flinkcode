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
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.e;

/**
 * Flink-窗口操作(OverWindow  从第一行到当前行开窗)
 */
public class FlinkSQL16_SQL_GroupWindow_Hop {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从端口读取数据，并转换每一行数据为JavaBean
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });
        WatermarkStrategy<WaterSensor> watermarkStrategy =
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(
                new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });
        SingleOutputStreamOperator<WaterSensor> waterSensorWaterMarkDS =
                waterSensorDS.assignTimestampsAndWatermarks(watermarkStrategy);


        // 将流转换为动态表,并指定那个处理时间字段
        Table table = tableEnv.fromDataStream(waterSensorWaterMarkDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // sql api是新滑动时间窗口
        // 观察官网写的方式
        Table result = tableEnv.sqlQuery("select " +
                "id," +
                "count(id) as ct," +
                "hop_start(pt,INTERVAL '2' second, INTERVAL '6' second) as windowStart from " + // 注意与排序的不同
                table +
                " group by id,hop(pt,INTERVAL '2' second, INTERVAL '6' second)");

        //tableEnv.toAppendStream(result,Row.class).print();
        result.execute().print();
        env.execute();

    }
}

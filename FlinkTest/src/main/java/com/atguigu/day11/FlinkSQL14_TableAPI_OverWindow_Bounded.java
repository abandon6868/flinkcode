package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.*;

/**
 * Flink-窗口操作(OverWindow  从第一行到当前行开窗)
 */
public class FlinkSQL14_TableAPI_OverWindow_Bounded {
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

        //4.开启Over往前有界窗口
        Table result = table.window(Over.partitionBy($("id"))  // 分区可选参数
                .orderBy($("pt"))  // 按照那么字段排序
                // .preceding(lit(5).seconds())  // preceding 参数，向前计算5S的，默认不设置，为无界流
                .preceding(rowInterval(2L)) // 计算本条加前两条的数据，计算三条数据
                    .as("ow"))
                .select($("id"),
                            $("vc").sum().over($("ow")));

        tableEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}

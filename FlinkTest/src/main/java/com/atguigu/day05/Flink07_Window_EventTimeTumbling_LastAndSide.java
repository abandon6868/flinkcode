package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink07_Window_EventTimeTumbling_LastAndSide {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(
                new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        // 提取数据中的时间戳字段

        // 老接口
        /*SingleOutputStreamOperator<WaterSensor> waterSensorWatermarkStrategy =
            waterSensorDS.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<WaterSensor>() {
            @Override
            public long extractAscendingTimestamp(WaterSensor element) {
                return element.getTs() * 1000L;
            }
        });*/

        // 自增的
        /*WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy =
                WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });*/

        // 乱序的，设置延时
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy =
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator  =
                waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        // 对数据进行分组
        KeyedStream<WaterSensor, String> keyedStream =
                waterSensorSingleOutputStreamOperator.keyBy(data -> data.getId());

        // 开窗
        WindowedStream<WaterSensor, String, TimeWindow> window =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                        .allowedLateness(Time.seconds(2))
                        .sideOutputLateData(new OutputTag<WaterSensor>("side"){});
        // 计算
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");
        result.print();
        DataStream<WaterSensor> sideOutput = result.getSideOutput(new OutputTag<WaterSensor>("side"){});
        sideOutput.print("side");

        env.execute(Flink07_Window_EventTimeTumbling_LastAndSide.class.getName());
    }
}

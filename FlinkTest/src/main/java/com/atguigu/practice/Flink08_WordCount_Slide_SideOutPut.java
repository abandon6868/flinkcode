package com.atguigu.practice;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink08_WordCount_Slide_SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        // 从waterSensor中提交时间 生成waterMark
        WatermarkStrategy<WaterSensor> watermarkStrategy =
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs() * 1000L;
                    }
                });

        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator =
                waterSensorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将waterMark 转换为Map
        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = singleOutputStreamOperator.map(new MapFunction<WaterSensor, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WaterSensor value) throws Exception {
                return new Tuple2<>(value.getId(), 1);
            }
        });

        // 对数据进行分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = outputStreamOperator.keyBy(data -> data.f0);

        // 开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window =
                keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("Side"){});

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);

        result.print("正流");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("Side"){});

        env.execute(Flink08_WordCount_Slide_SideOutPut.class.getName());
    }
}

/***
 *  使用事件事件处理数据，从端口获取数据实现，
 *  每隔5s计算最近30s的每隔传感器发送的水位线，
 *  WatreMark设置延迟2秒钟
 *  允许迟到2s，将迟到的数据放到侧输出流中
 */
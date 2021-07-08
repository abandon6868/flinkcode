package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class Flink09_Window_EventTimeSession {
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

        // 开窗,当到达时间间隔，session的窗口才会关闭，waterMark的与数据本身的差值，大于等于时间间隔
        WindowedStream<WaterSensor, String, TimeWindow> window =
                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(5)));
        // 计算
        SingleOutputStreamOperator<WaterSensor> result = window.sum("vc");
        result.print();

        env.execute(Flink09_Window_EventTimeSession.class.getName());
    }
}

/**
 * 第一次测试：结果为2   010 watermark值为1577844007 1577844010 - 1577844002 =5 符合触发条件
*  ws_001,1577844001,1
 * ws_001,1577844002,1
 * ws_001,1577844010,1
 * 第二次测试，无结果  009 watermark值为1577844006 1577844006 - 1577844002 =4 不符合触发条件
 * ws_001,1577844001,1
 * ws_001,1577844002,1
 * ws_001,1577844009,1
 * 第三次测试：结果为3 017 watermark值为1577844014 1577844014 - 1577844009 =5 符合触发条件
 * ws_001,1577844001,1
 * ws_001,1577844002,1
 * ws_001,1577844009,1
 * ws_001,15778440017,1
**/
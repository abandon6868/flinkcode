package com.atguigu.day07;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

// 网站总浏览量（PV）的统计 此方式会出现数据倾斜
public class Flink01_Practice_PageView_Window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> UserBehaviorDS =
                dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4])
                );
                return userBehavior;
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS =
                UserBehaviorDS.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        // 通过 UserBehaviorDS 提取时间，生成watermark
        WatermarkStrategy<UserBehavior> watermarkStrategy =
                WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().
                withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        SingleOutputStreamOperator<UserBehavior> timestampsAndWatermarks =
                userBehaviorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将数据转换为元组
        SingleOutputStreamOperator<Tuple2<String, Long>> StreamOperator =
                timestampsAndWatermarks.map(
                        new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return new Tuple2<>("pv", 1L);
            }
        });

        // 分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream =
                StreamOperator.keyBy(data -> data.f0);

        // 开窗
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> window =
                keyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        SingleOutputStreamOperator<Tuple2<String, Long>> result = window.sum(1);

        result.print();

        env.execute(Flink01_Practice_PageView_Window.class.getName());
    }
}

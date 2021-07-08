package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink10_Window_EventTimeTumbling_CustomerPeriod {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS  =
                dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        // 使用自定义waterMark自定义提取时间
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPeriod(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });


        SingleOutputStreamOperator<WaterSensor> assignTimestampsAndWatermarks =
                waterSensorDS.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        KeyedStream<WaterSensor, String> keyedStream = assignTimestampsAndWatermarks.keyBy(data -> data.getId());

        WindowedStream<WaterSensor, String, TimeWindow> window =
                keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
        window.sum("vc").print();


        env.execute(Flink10_Window_EventTimeTumbling_CustomerPeriod.class.getName());

    }

    //自定义周期性的Watermark生成器
    public static class MyPeriod implements WatermarkGenerator<WaterSensor>{

        private Long maxTS;
        // 允许的最大延迟时间 ms
        private Long maxDelay;

        public MyPeriod(Long maxDelay) {
            this.maxTS = Long.MIN_VALUE + maxDelay +1;
            this.maxDelay = maxDelay;
        }

        // 每收到一个元素, 执行一次. 用来生产WaterMark中的时间戳
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("取数据中最大的时间戳");
            maxTS = Math.max(eventTimestamp,maxTS);
        }

        // 周期性的把WaterMark发射出去, 默认周期是200ms
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("生成WaterMark" + (maxTS - maxDelay));
            output.emitWatermark(new Watermark(maxTS - maxDelay));
        }
    }

}

package com.atguigu.day08;

import com.atguigu.bean.AdCount;
import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/***
 * 每5秒统计一次1小时内各省份的广告点击数
 *
 */

public class Flink03_Practice_AdCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据
        DataStreamSource<String> dataStreamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\AdClickLog.csv");

        // 转换为javabean，并提取时间戳
        SingleOutputStreamOperator<AdsClickLog> adsClickLogDS  = dataStreamSource.map(new MapFunction<String, AdsClickLog>() {
            @Override
            public AdsClickLog map(String value) throws Exception {
                String[] split = value.split(",");
                AdsClickLog adsClickLog = new AdsClickLog(
                        Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        split[2],
                        split[3],
                        Long.parseLong(split[4])
                );
                return adsClickLog;
            }
        });

        WatermarkStrategy<AdsClickLog> watermarkStrategy = WatermarkStrategy.<AdsClickLog>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<AdsClickLog>() {
            @Override
            public long extractTimestamp(AdsClickLog element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<AdsClickLog> timestampsAndWatermarks = adsClickLogDS.assignTimestampsAndWatermarks(watermarkStrategy);

        KeyedStream<AdsClickLog, String> provinceKeyedStream = timestampsAndWatermarks.keyBy(AdsClickLog::getProvince);

        SingleOutputStreamOperator<AdCount> result = provinceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggregateFunc(), new AdCountWindowFunc());

        result.print();

        env.execute(Flink03_Practice_AdCount.class.getName());
    }

    private static class AdCountAggregateFunc implements AggregateFunction<AdsClickLog, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(AdsClickLog value, Long accumulator) {
            return accumulator +1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a +b ;
        }
    }

    private static class AdCountWindowFunc implements WindowFunction<Long, AdCount,String, TimeWindow> {
        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input,
                          Collector<AdCount> out) throws Exception {
            // 获取数据并输出
            Long next = input.iterator().next();

            // 输出数据
            long windowEndTs = window.getEnd();
            out.collect(new AdCount(key,windowEndTs,next));
        }
    }
}

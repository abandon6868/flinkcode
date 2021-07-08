package com.atguigu.day08;

import com.atguigu.bean.AdCount;
import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;

/***
 * 每5秒统计一次1小时内各省份的广告点击数
 * 并将点击次数过多的用户加入黑名单 按照用户和广告id分组
 *
 */

public class Flink04_Practice_AdCount2 {
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

        // 分组:按照省份分组
        KeyedStream<AdsClickLog, String> provinceKeyedStream = timestampsAndWatermarks.keyBy(AdsClickLog::getProvince);

        // 分组：按照user和广告id进行发分组
        KeyedStream<AdsClickLog, String> adsClickLogStringKeyedStream = timestampsAndWatermarks.keyBy(data -> data.getUserId() + "_" + data.getAdId());

        // 开窗计算：每个用户的对每个广告点击量
        // 需要使用窗口，状态+定时器
        SingleOutputStreamOperator<AdsClickLog> clickLogSingleOutputStreamOperator = adsClickLogStringKeyedStream.process(new BlackListProcessFunc(100L));
        // 开窗计算: 每个省份的点击量
        SingleOutputStreamOperator<AdCount> result = provinceKeyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(5)))
                .aggregate(new AdCountAggregateFunc(), new AdCountWindowFunc());

        result.print();
        clickLogSingleOutputStreamOperator.getSideOutput(new OutputTag<String>("blackList"){}).print("SideOutPut");

        env.execute(Flink04_Practice_AdCount2.class.getName());
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

    private static class BlackListProcessFunc extends KeyedProcessFunction<
            String, AdsClickLog, AdsClickLog> {
        // 定义最大的点击次数
        private Long maxClickCount;
        // 定义状态
        private ValueState<Long> clickCountState;  // 点击次数
        private ValueState<Boolean> isSendState;  // 判断消息是否发送过,如果不设置这个状态，点击次数过多会一直重复发送


        public BlackListProcessFunc(Long maxClickCount) {
            this.maxClickCount = maxClickCount;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            clickCountState = getRuntimeContext().getState(new ValueStateDescriptor<Long>(
                    "clickCount-State",Long.class));
            isSendState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Boolean>("is-SendState",Boolean.class));
        }

        @Override
        public void processElement(AdsClickLog value, Context ctx, Collector<AdsClickLog> out) throws Exception {
            // 从状态中获取数据
            Long clickCount = clickCountState.value();
            Boolean isSend = isSendState.value();

            // 如果是第一条数据，更新状态，并注册定时器
            if (clickCount == null){
                clickCountState.update(1L);
                // 注册定时器,东八区，需要减8 得到0时区
                long ts = (value.getTimestamp() / (60 * 60 * 24) + 1) * (60 * 60 * 24 * 1000L) - 8 * 60 * 60 * 1000L;
                System.out.println(new Timestamp(ts));
                ctx.timerService().registerProcessingTimeTimer(ts);
            }else {
                clickCount +=1;
                if(clickCount>=maxClickCount){
                    if (isSend == null){
                        ctx.output(new OutputTag<String>("blackList"){},
                                value.getUserId() + "点击了" + value.getAdId()
                                        + "广告次数" + maxClickCount+",存在恶意点击");
                        //更新黑名单状态
                        isSendState.update(true);
                    }
                    return;
                }
            }
            out.collect(value);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<AdsClickLog> out) throws Exception {
            clickCountState.clear();
            isSendState.clear();
        }
    }
}

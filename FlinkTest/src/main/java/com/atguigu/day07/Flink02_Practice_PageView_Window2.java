package com.atguigu.day07;

import com.atguigu.bean.PageViewCount;
import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Random;

// 网站总浏览量（PV）的统计 此方式结局数据倾斜问题
public class Flink02_Practice_PageView_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorMapDS =
                dataStreamSource.map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]));
                        return userBehavior;
                    }
                });

        // 提取时间戳吗，生成watermark
        WatermarkStrategy<UserBehavior> watermarkStrategy =
                WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                            @Override
                            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                                return element.getTimestamp() * 1000L;
                            }
                        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS =
                userBehaviorMapDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将数据转换成map，并过滤 pv
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilterDS =
                userBehaviorDS.filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                });

        // 给每个key加个随机数，避免数据倾斜
        SingleOutputStreamOperator<Tuple2<String, Long>> PVToOneDS = userBehaviorFilterDS.map(
                new MapFunction<UserBehavior, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                        return new Tuple2<>("pv_" + new Random().nextInt(8), 1L);
                    }
                });

        // 对数据进行分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = PVToOneDS.keyBy(data -> data.f0);

        // 开窗计算
        SingleOutputStreamOperator<PageViewCount> aggregateWindow = keyedStream.window(TumblingEventTimeWindows
                .of(Time.hours(1)))
                .aggregate(new PvCountAggregateFunc(), new PvCountProcessFunc());

        // 按照窗口信息重新分组做第二次聚合
        KeyedStream<PageViewCount, String> stringKeyedStream =
                aggregateWindow.keyBy(PageViewCount::getTime);

        // 第一种计算结果，繁琐，打印结果太多
        // stringKeyedStream.sum("PvCount").print();

        SingleOutputStreamOperator<PageViewCount> process =
                stringKeyedStream.process(new PageViewProcessFunc());

        process.print();

        env.execute(Flink02_Practice_PageView_Window2.class.getName());
    }

    private static class PvCountAggregateFunc implements AggregateFunction<Tuple2<String, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<String, Long> value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a+b;
        }
    }

    private static class PvCountProcessFunc implements WindowFunction<Long, PageViewCount, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<PageViewCount> out) throws Exception {
            // 从窗口中获取窗口结束时间
            String windowEndTime = new Timestamp(window.getEnd()).toString();

            // 获取累积结果
            Long pvCount = input.iterator().next();

            out.collect(new PageViewCount("Pv",windowEndTime,pvCount));
        }
    }


    private static class PageViewProcessFunc extends KeyedProcessFunction<String, PageViewCount, PageViewCount>  {
        // 定义状态
        private ListState<PageViewCount> pvState;

        // 初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            pvState = getRuntimeContext().getListState(
                    new ListStateDescriptor<PageViewCount>("pv-State",PageViewCount.class));
        }

        @Override
        public void processElement(PageViewCount value, Context ctx, Collector<PageViewCount> out) throws Exception {
            //1 将数据添加进状态
            pvState.add(value);
            // 2 设置定时器
            String eventTime = value.getTime();
            long time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(eventTime).getTime();
            ctx.timerService().registerEventTimeTimer(time +1 );
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<PageViewCount> out) throws Exception {
            // 出发定时器
            Iterable<PageViewCount> pageViewCounts = pvState.get();
            Iterator<PageViewCount> iterator = pageViewCounts.iterator();

            Long pageViewCount = 0L;
            while (iterator.hasNext()) {
                pageViewCount += iterator.next().getPvCount();
            }

            // 清空状态
            pvState.clear();
            // 删除定时器
            ctx.timerService().deleteEventTimeTimer(timestamp);

            out.collect(new PageViewCount("pv",new Timestamp(timestamp -1).toString(),pageViewCount));
        }
    }
}

package com.atguigu.day08;

import com.atguigu.bean.ApacheLog;
import com.atguigu.bean.UrlCount;
import org.apache.commons.compress.utils.Lists;
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
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

/**
 * 基于服务器log的热门页面浏览量统计
 * 每隔5秒，输出最近10分钟内访问量最多的前N个URL
 */
public class Flink01_Practice_HotUrl {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\apache.log");

        // 转换为javaBean
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
        SingleOutputStreamOperator<ApacheLog> apacheDS =
                streamSource.map(new MapFunction<String, ApacheLog>() {
            @Override
            public ApacheLog map(String value) throws Exception {
                String[] split = value.split(" ");
                ApacheLog apacheLog = new ApacheLog(
                        split[0],
                        split[1],
                        sdf.parse(split[3]).getTime(),
                        split[5],
                        split[6]
                );
                return apacheLog;
            }
        }).filter(new FilterFunction<ApacheLog>() {
            @Override
            public boolean filter(ApacheLog value) throws Exception {
                return "GET".equals(value.getMethod());
            }
        });

        // 生成waterMark
        WatermarkStrategy<ApacheLog> watermarkStrategy =
                WatermarkStrategy.<ApacheLog>forBoundedOutOfOrderness(Duration.ofSeconds(2))  // 观察乱想时间差值，这里为了观察效果，将时间设置较小
                        .withTimestampAssigner(new SerializableTimestampAssigner<ApacheLog>() {
                            @Override
                            public long extractTimestamp(ApacheLog element, long recordTimestamp) {
                                return element.getTs();
                            }
                        });
        SingleOutputStreamOperator<ApacheLog> timestampsAndWatermarks =
                apacheDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 对数据做map操作
        SingleOutputStreamOperator<Tuple2<String, Long>> singleOutputStreamOperator =
                timestampsAndWatermarks.map(new MapFunction<ApacheLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(ApacheLog value) throws Exception {
                return new Tuple2<>(value.getUrl(), 1L);
            }
        });

        // 对数据进行分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = singleOutputStreamOperator.keyBy(data -> data.f0);


        // 对数据开窗进行计算
        SingleOutputStreamOperator<UrlCount> aggregate = keyedStream.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(new OutputTag<Tuple2<String, Long>>("sideOutPut") {
                })
                .aggregate(new UrlTopNCountAggregateFunc(), new UrlTopNCountProcessFunc());

        // 对数据进行二次分组
        KeyedStream<UrlCount, Long> urlCountLongKeyedStream = aggregate.keyBy(UrlCount::getWindowEnd);

        SingleOutputStreamOperator<String> result =
                urlCountLongKeyedStream.process(new UrlTopNCountKeyedProcessFunc(5));



        result.print();

        env.execute(Flink01_Practice_HotUrl.class.getName());
    }

    private static class UrlTopNCountAggregateFunc implements AggregateFunction<Tuple2<String, Long>,Long,Long> {
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
            return a +b;
        }
    }

    private static class UrlTopNCountProcessFunc implements WindowFunction<Long, UrlCount, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow window, Iterable<Long> input, Collector<UrlCount> out)
                throws Exception {
            // 从迭代器中读取数据
            Long next = input.iterator().next();

            // 输出结果
            out.collect(new UrlCount(key,
                    window.getEnd(),
                    next));
        }
    }

    private static class UrlTopNCountKeyedProcessFunc extends KeyedProcessFunction<Long, UrlCount, String> {

        private Integer TopN;

        public UrlTopNCountKeyedProcessFunc(Integer topN) {
            TopN = topN;
        }

        /**
         * 使用listState存在Bug： 当在socket 接收数据时发现
         *  乱序数据 当前窗口假如来三天数据 A B A
         *  liststate 中数据就是 (A,1)  (B,1)  (A,2) 会出现重复数据
         *  要进行去重操作，使用mapState 函数
         */

        // 使用list状态
        private ListState<UrlCount> listState;

        // 初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlCount>("list-State",UrlCount.class));
        }

        @Override
        public void processElement(UrlCount value, Context ctx, Collector<String> out) throws Exception {
            // 将数据添加进listState
            listState.add(value);

            // 注册定时器
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() +1000L);
            // 注册窗口关闭定时器，因为允许出现迟到数据，需要在迟到数据延迟后的基础上关闭窗口
            ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 6000L +1L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 取出状态数据
            Iterator<UrlCount> iterator = listState.get().iterator();
            ArrayList<UrlCount> urlCounts = Lists.newArrayList(iterator);

            urlCounts.sort(new Comparator<UrlCount>() {
                @Override
                public int compare(UrlCount o1, UrlCount o2) {
                    return (int)(o2.getCount() - o1.getCount());
                }
            });

            StringBuilder builder = new StringBuilder();
            builder.append("===========")
                    .append(new Timestamp(timestamp - 1L))
                    .append("===========")
                    .append("\n");

            for (Integer i = 0; i < Math.min(TopN,urlCounts.size()); i++) {
                UrlCount urlCount = urlCounts.get(i);
                builder.append("Top:").append(i+1)
                        .append(" URL:").append(urlCount.getUrl())
                        .append(" Count:").append(urlCount.getCount())
                        .append("\n");
            }

            builder.append("===========")
                    .append(new Timestamp(timestamp - 1L))
                    .append("===========")
                    .append("\n");

            // 删除定时器
            ctx.timerService().deleteEventTimeTimer(timestamp);
            // 清空状态
            if (timestamp == ctx.getCurrentKey() + 6000L +1L){
                listState.clear();
                // 删除定时器
                ctx.timerService().deleteEventTimeTimer(timestamp);
                return;
            }
            // 输出数据
            out.collect(builder.toString());

            Thread.sleep(1000L);

        }
    }
}

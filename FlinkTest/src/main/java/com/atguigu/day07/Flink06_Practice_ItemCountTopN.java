package com.atguigu.day07;

import com.atguigu.bean.ItemCount;
import com.atguigu.bean.UserBehavior;
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
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;


/**
 * 没隔5分钟输出最近1小时内点击两最多的前N个商品
 */

public class Flink06_Practice_ItemCountTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS =
                streamSource.map(new MapFunction<String, UserBehavior>() {
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
        })
                        .filter(new FilterFunction<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior value) throws Exception {
                        return "pv".equals(value.getBehavior());
                    }
                });

        // 提取时间，生成waterMark
        WatermarkStrategy<UserBehavior> watermarkStrategy =
                WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UserBehavior> timestampsAndWatermarks =
                userBehaviorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将数据转成map类型，只保留有效数据
        SingleOutputStreamOperator<Tuple2<Long, Long>> streamOperator =
                timestampsAndWatermarks.map(new MapFunction<UserBehavior, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> map(UserBehavior value) throws Exception {
                return new Tuple2<>(value.getItemId(), 1L);
            }
        });

        // 对数据进行分组
        KeyedStream<Tuple2<Long, Long>, Long> keyedStream = streamOperator.keyBy(data -> data.f0);

        // 对数据进行开窗计算
        WindowedStream<Tuple2<Long, Long>, Long, TimeWindow> windowedStream =
                keyedStream.window(SlidingEventTimeWindows.of(Time.hours(1), Time.minutes(5)));

        SingleOutputStreamOperator<ItemCount> aggregate =
                windowedStream.aggregate(new ItemCountAggregateFunc(), new ItemCountWindowFunc());

        // 按照窗口信息，重新分组，实现TopN
        KeyedStream<ItemCount, String> countStringKeyedStream = aggregate.keyBy(ItemCount::getTime);

        SingleOutputStreamOperator<String> result =
                countStringKeyedStream.process(new ItemCountKeyedFunc(5));

        result.print();
        env.execute();
    }

    private static class ItemCountAggregateFunc implements AggregateFunction<Tuple2<Long, Long>, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Tuple2<Long, Long> value, Long accumulator) {
            return accumulator +1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }

    private static class ItemCountWindowFunc implements WindowFunction<Long, ItemCount, Long, TimeWindow> {
        @Override
        public void apply(Long itemId, TimeWindow window, Iterable<Long> input, Collector<ItemCount> out)
                throws Exception {
            // 取出数据
            Long next = input.iterator().next();

            // 输出数据
            out.collect(new ItemCount(itemId,
                    new Timestamp(window.getEnd()).toString(),
                    next));
        }
    }

    private static class ItemCountKeyedFunc extends KeyedProcessFunction<String,ItemCount,String> {
        private ListState<ItemCount> listState;
        private Integer TopSize;

        public ItemCountKeyedFunc(Integer topSize) {
            TopSize = topSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ItemCount>("list-State",ItemCount.class));
        }

        @Override
        public void processElement(ItemCount value, Context ctx, Collector<String> out) throws Exception {
            // 添加数据
            listState.add(value);

            // 设置定时器
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            ctx.timerService().registerEventTimeTimer(
                    simpleDateFormat.parse(value.getTime()).getTime() + 1000L);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 从状态中获取数据
            Iterator<ItemCount> iterator = listState.get().iterator();

            // 将 iterator转换成 list
            ArrayList<ItemCount> itemCountArrayList = Lists.newArrayList(iterator);
            // 对数据进行排序
            itemCountArrayList.sort(new Comparator<ItemCount>() {
                @Override
                public int compare(ItemCount o1, ItemCount o2) {
                    return (int) (o2.getCount() -o1.getCount());
                }
            });

            StringBuilder builder = new StringBuilder();
            builder.append(("==========="))
                    .append(new Timestamp(timestamp -1000L))
                    .append("===========")
                    .append("\n");

            for (Integer i = 0; i < Math.min(TopSize,itemCountArrayList.size()); i++) {
                ItemCount itemCount = itemCountArrayList.get(i);

                builder.append("TOP").append(i+1);
                builder.append(" ItemId").append(itemCount.getItem())
                        .append(" Count").append(itemCount.getCount())
                        .append("\n");
            }
            builder.append(("==========="))
                    .append(new Timestamp(timestamp -1000L))
                    .append("===========")
                    .append("\n");

            // 删除定时器
            ctx.timerService().deleteEventTimeTimer(timestamp -1000L);
            // 清空状态
            listState.clear();
            // 输出结果

            Thread.sleep(2000L);
            out.collect(builder.toString());

        }
    }
}

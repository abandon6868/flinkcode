package com.atguigu.day07;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;

/***
 *  计算一小时的userVisitor 数量
 *  统计流量的重要指标是网站的独立访客数（Unique Visitor，UV）
 */

public class Flink03_Practice_UserVisitor_Window {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // 从文本中读取数据
        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\UserBehavior.csv");
        
        // 将数据转换成map,并过滤数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilterDS = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]),
                        split[3],
                        Long.parseLong(split[4]));
                return userBehavior;
            }
        }).filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });

        // 提取时间戳，生成waterMark
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator =
                userBehaviorFilterDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 对数据按照用户行为进行分组
        KeyedStream<UserBehavior, String> userBehaviorStringKeyedStream =
                userBehaviorSingleOutputStreamOperator.keyBy(UserBehavior::getBehavior);

        // 开窗计算
        WindowedStream<UserBehavior, String, TimeWindow> windowedStream  =
                userBehaviorStringKeyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        SingleOutputStreamOperator<UserVisitorCount> process =
                windowedStream.process(new UserVisitProcessWindowFunc());

        process.print();

        env.execute();
    }

    // 使用hashSet进行去重计算
    private static class UserVisitProcessWindowFunc extends ProcessWindowFunction<UserBehavior, UserVisitorCount,String,TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<UserBehavior>
                elements, Collector<UserVisitorCount> out) throws Exception {
            // 创建hastSet 进行去重
            HashSet<Long> hashSet = new HashSet<>();

            // 从窗口中取出数据,并添加至hashSet
            Iterator<UserBehavior> iterator = elements.iterator();
            while (iterator.hasNext()) {
                hashSet.add(iterator.next().getUserId());
            }

            //  输出数据
            out.collect(new UserVisitorCount("UV",
                    new Timestamp(context.window().getEnd()).toString(),
                    hashSet.size()));
        }
    }
}

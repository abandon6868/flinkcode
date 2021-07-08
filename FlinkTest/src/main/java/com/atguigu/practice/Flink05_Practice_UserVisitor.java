package com.atguigu.practice;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Random;
// 网站总浏览量（PV）的统计
public class Flink05_Practice_UserVisitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> userVisitorDS = dataStreamSource.flatMap(
                new FlatMapFunction<String, UserBehavior>() {
                    @Override
                    public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                        String[] infos = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(Long.parseLong(infos[0]),
                                Long.parseLong(infos[1]),
                                Integer.parseInt(infos[2]),
                                infos[3] +"_" +new Random().nextInt(100),
                                Long.parseLong(infos[4]));
                        if (userBehavior.getBehavior().contains("pv")) {
                            out.collect(userBehavior);
                        }
                    }
                });

        KeyedStream<UserBehavior, String> keyedStream = userVisitorDS.keyBy(data -> "UV");
        // 使用process的方式计算求和
        SingleOutputStreamOperator<Long> result = keyedStream.process(
                new KeyedProcessFunction<String, UserBehavior, Long>() {
            private HashSet uIds = new HashSet<>();
            private Long count = 0L;

            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Long> out)
                    throws Exception {
                if (!uIds.contains(value.getUserId())) {
                    uIds.add(value.getUserId());
                    count++;
                    out.collect(count);
                }
            }
        });

        result.print();

        env.execute(Flink05_Practice_UserVisitor.class.getName());
    }
}

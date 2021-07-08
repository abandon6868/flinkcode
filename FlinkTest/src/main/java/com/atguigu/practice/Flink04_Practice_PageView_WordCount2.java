package com.atguigu.practice;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.security.Key;

public class Flink04_Practice_PageView_WordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<UserBehavior> pv = dataStreamSource.flatMap(
                new FlatMapFunction<String, UserBehavior>() {
            @Override
            public void flatMap(String value, Collector<UserBehavior> out) throws Exception {
                String[] infos = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(infos[0]),
                        Long.parseLong(infos[1]),
                        Integer.parseInt(infos[2]),
                        infos[3],
                        Long.parseLong(infos[4]));
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(userBehavior);
                }
            }
        });

        KeyedStream<UserBehavior, String> keyedStream = pv.keyBy(data -> data.getBehavior());


        SingleOutputStreamOperator<Long> result = keyedStream.process(
                new ProcessFunction<UserBehavior, Long>() {
            Long count = 0L;
            @Override
            public void processElement(UserBehavior value, Context ctx, Collector<Long> out) throws Exception {
                count++;
                out.collect(count);
            }
        });

        result.print();

        env.execute(Flink04_Practice_PageView_WordCount2.class.getName());

    }
}

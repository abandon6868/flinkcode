package com.atguigu.practice;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink04_Practice_PageView_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");

        SingleOutputStreamOperator<Tuple2<String, Integer>> pv = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] infos = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(infos[0]),
                        Long.parseLong(infos[1]),
                        Integer.parseInt(infos[2]),
                        infos[3],
                        Long.parseLong(infos[4]));
                if ("pv".equals(userBehavior.getBehavior())) {
                    out.collect(new Tuple2<>("pv", 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Object> keyedStream = pv.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        result.print();

        env.execute(Flink04_Practice_PageView_WordCount.class.getName());
    }
}

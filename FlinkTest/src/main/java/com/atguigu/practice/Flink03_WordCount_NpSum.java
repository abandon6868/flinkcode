package com.atguigu.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;

public class Flink03_WordCount_NpSum {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String word : split) {
                    out.collect(new Tuple2<>(word, 1));
                }

            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = streamOperator.keyBy(x -> x.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result  = keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            HashMap<String, Integer> hashMap = new HashMap<>();

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx,
                                       Collector<Tuple2<String, Integer>> out) throws Exception {
                Integer count = hashMap.getOrDefault(value.f0, 0);
                count++;
                hashMap.put(value.f0, count);
                out.collect(new Tuple2<>(value.f0, count));
            }
        });



/*        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {

                return new Tuple2<>(value1.f0, value1.f1 + 1);
            }
        });*/

        result.print();

        env.execute("Flink03_WordCount_NpSum");
    }
}

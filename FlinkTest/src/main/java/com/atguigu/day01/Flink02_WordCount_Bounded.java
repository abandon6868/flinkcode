package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 有界流
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1); // 设置并行度

        //2 读取文件
        DataStreamSource<String> input = env.readTextFile("input");

        // 3 压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDs = input.flatMap(new LineToFlatMapFunc());

        // 4 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOneDs.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        // 5 按照key做聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6 打印结果
        result.print();

        // 7 启动任务
        env.execute(Flink02_WordCount_Bounded.class.getName());

    }

    public static class LineToFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split(" ");

            for (String word : words) {
                collector.collect(new Tuple2<>(word,1));
            }

        }
    }
}

/** 设置并行度后打印
 * (hello,1)
 * (spark,1)
 * (hello,2)
 * (flink,1)
 * (hello,3)
 * (hive,1)
 *
 * 不设置并行度
 * 1> (spark,1)
 * 1> (hive,1)
 * 3> (hello,1)
 * 7> (flink,1)
 * 3> (hello,2)
 * 3> (hello,3)
 * */
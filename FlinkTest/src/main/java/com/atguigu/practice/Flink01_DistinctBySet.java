package com.atguigu.practice;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.hadoop.mapred.lib.aggregate.ValueAggregator;

import java.util.HashSet;

public class Flink01_DistinctBySet {
    public static void main(String[] args) throws Exception {

        HashSet<String> hashSet = new HashSet<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1，但是效率不高，问题不设置并行度，默认并行度为8,flink采用轮询的方式，会出现重复元素
        // 解决方案： 使用keyby 将相同元素的分发到一个slot中
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> wordDS = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        wordDS.keyBy(x->x).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (hashSet.contains(value)){
                    return false;
                }else {
                    hashSet.add(value);
                    return true;
                }

            }
        }).print();

        env.execute(Flink01_DistinctBySet.class.getName());
    }
}

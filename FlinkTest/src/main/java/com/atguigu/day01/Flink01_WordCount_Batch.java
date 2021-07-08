package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


public class Flink01_WordCount_Batch {

    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 2 读取文件数据
        DataSource<String> input = environment.readTextFile("input");

        // 3 压平
        FlatMapOperator<String, String> wordDS = input.flatMap(new MyFlatMapFunc());

        // 4 map 将单次转换为元组
        MapOperator<String, Tuple2<String, Integer>> wordToOneDS = wordDS.map(
                new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });

        // 5 分组
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOneDS.groupBy(0);
        // 6 聚合
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        // 7 打印结果
        result.print();

    }

    // 自定义flatmap
    public static class MyFlatMapFunc implements FlatMapFunction<String,String>{

        @Override
        public void flatMap(String s, Collector<String> collector) throws Exception {
            String[] words = s.split(" ");
            for (String word : words) {
                collector.collect(word);
            }


        }
    }


}

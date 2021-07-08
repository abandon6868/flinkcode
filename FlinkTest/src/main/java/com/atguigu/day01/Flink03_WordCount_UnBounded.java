package com.atguigu.day01;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink 并行读优先级问题：
 * 1 代码中算子单独问题
 * 2 代码中env全局设置
 * 3 提交参数
 * 4 默认配置信息
 */
public class Flink03_WordCount_UnBounded {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        // 2 读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        // 3 将每行数据压平并转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOnes = socketTextStream.flatMap(
                new Flink02_WordCount_Bounded.LineToFlatMapFunc());

        // 4 分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOnes.keyBy(
                new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });

        // 5 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        // 6 打印
        result.print();

        // 7 启动
        env.execute(Flink03_WordCount_UnBounded.class.getName());
    }
}

/**
 *  启动 nc -lk 9999
 *
 */

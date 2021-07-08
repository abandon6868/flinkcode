package com.atguigu.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class Flink08_State_WordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置状态后端，并开启checkpoint
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/flink_ck"));
        env.enableCheckpointing(5000L);  // checkpoint 时间点
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE); // 精准一次
        // cancal 任务时，不删除checkpoint目录
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        // 将数据转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToTupleDs = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        // 对数据进行分组计算
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToTupleDs.keyBy(data -> data.f0);

        // 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        // 执行任务
        env.execute(Flink08_State_WordCount.class.getName());
    }
}

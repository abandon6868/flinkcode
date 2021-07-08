package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink13_TransForm_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从端口获取数据流
        DataStreamSource<String> streamSource1 = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource2 = env.socketTextStream("hadoop102", 8888);

        DataStream<String> union = streamSource1.union(streamSource2);

        union.print();

        env.execute(Flink13_TransForm_Union.class.getName());
    }
}

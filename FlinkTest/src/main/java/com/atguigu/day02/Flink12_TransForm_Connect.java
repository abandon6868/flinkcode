package com.atguigu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink12_TransForm_Connect {
    public static void main(String[] args) throws Exception {
        // 1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2 从端口读取数据创建流
        DataStreamSource<String> stringDS = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 8888);

        // 3 将 streamSource1 字符串流转换为integer 类型
        SingleOutputStreamOperator<Integer> intDS = streamSource.map((MapFunction<String, Integer>) value -> value.length());

        // 4 将不同类型的两个流进行连接
        ConnectedStreams<String, Integer> connectDS = stringDS.connect(intDS);

        // 5 处理连接后的流
        SingleOutputStreamOperator<Object> result = connectDS.map(new CoMapFunction<String, Integer, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value;
            }
        });

        // 6 打印数据
        result.print();
        // 7 开始执行
        env.execute(Flink12_TransForm_Connect.class.getName());
    }
}

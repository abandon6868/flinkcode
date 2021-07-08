package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink11_TransForm_RichFilterFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        // 过滤数据，取水位高于30的
        SingleOutputStreamOperator<String> filter = streamSource.filter(new MyRichFilterFunc());

        filter.print();

        env.execute(Flink11_TransForm_RichFilterFunction.class.getName());

    }

    private static class MyRichFilterFunc extends RichFilterFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("开始执行open方法");
        }

        @Override
        public boolean filter(String value) throws Exception {
            String[] split = value.split(",");
            return Integer.parseInt(split[2])>30;
        }

        @Override
        public void close() throws Exception {
            System.out.println("开始执行 close 方法");
        }
    }
}

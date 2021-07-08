package com.atguigu.day02;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink10_TransForm_RichFlatMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<String> streamOperator = streamSource.flatMap(new MyRichFlatMap());

        streamOperator.print();


        env.execute(Flink10_TransForm_RichFlatMapFunction.class.getName());
    }

    private static class MyRichFlatMap extends RichFlatMapFunction<String,String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("开始执行 open方法");
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {
            String[] split = value.split(",");
            for (String s : split) {
                out.collect(s);
            }
        }

        @Override
        public void close() throws Exception {
            System.out.println("开始执行 close方法");
        }
    }
}

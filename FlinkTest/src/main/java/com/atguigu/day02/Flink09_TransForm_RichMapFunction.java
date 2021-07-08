package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink09_TransForm_RichMapFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<WaterSensor> waterSensorStream = streamSource.map(new MyRichMapFunc());

        waterSensorStream.print();

        env.execute(Flink09_TransForm_RichMapFunction.class.getName());
    }

    /**
     * Rich 富有的地方：
     *   1 生命周期的方法
     *   2 可以获取上下文执行环境，做状态编程
     */
    private static class MyRichMapFunc extends RichMapFunction<String, WaterSensor> {

        @Override
        // 每个并行度里面都会有一个open方法
        public void open(Configuration parameters) throws Exception {
            // 生命周期方法，一般用于库的建立和连接
            System.out.println("open 方法被执行");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            String[] split = value.split(",");
            WaterSensor waterSensor = new WaterSensor(split[0],
                    Long.parseLong(split[1]), Integer.parseInt(split[2]));
            return waterSensor;
        }

        @Override
        public void close() throws Exception {
            // 关闭
            System.out.println("close方法被调用");
        }
    }
}

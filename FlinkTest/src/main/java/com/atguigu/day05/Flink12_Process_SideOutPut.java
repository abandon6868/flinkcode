package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink12_Process_SideOutPut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");

                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        // 使用process将数据分流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorDS.process(new SplitFunc());

        // 输出结果
        result.print("主流");
        DataStream<Tuple2<String, Integer>> sideOutput =
                result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOut") {
        });

        sideOutput.print("侧输出流");

        env.execute();
    }

    public static class SplitFunc extends ProcessFunction<WaterSensor, WaterSensor> {
        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out)
                throws Exception {
            // 根据水位线的高低实现分流
            if (value.getVc() >=30){
                // 将数据写入主流
                out.collect(value);
            }else {
                // 数据输出到侧流，传入两个参数，第一个为侧输出流的返回类型，第二个为侧输出流的返回值
                ctx.output(new OutputTag<Tuple2<String,Integer>>("SideOut"){},
                        new Tuple2<>(value.getId(),value.getVc()));
            }

        }
    }
}

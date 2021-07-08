package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink14_TransForm_Max {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] infos = value.split(",");
                WaterSensor waterSensor = new WaterSensor(infos[0],
                        Long.parseLong(infos[1]), Integer.parseInt(infos[2]));
                out.collect(waterSensor);
            }
        });

        // 按照传感器ID 进行分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(
                new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        // 按照水位线进行计算
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.max("vc");

        result.print();


        env.execute(Flink14_TransForm_Max.class.getName());
    }
}

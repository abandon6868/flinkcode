package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink15_TransForm_MaxBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.flatMap(
                new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] infos = value.split(",");
                out.collect(new WaterSensor(infos[0], Long.parseLong(infos[1]),
                        Integer.parseInt(infos[2])));
            }
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(
                new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor value) throws Exception {
                return value.getId();
            }
        });

        keyedStream.maxBy("vc").print();

        env.execute(Flink15_TransForm_MaxBy.class.getName());
    }
}

/***
 *  max与maxBy的区别
 *  max: 按照指定字段分组，分组后按照另一个指定的字段取该字段最大值
 *  maxBy: 按照指定字段分组，分组后取该条记录作为最大值
 */
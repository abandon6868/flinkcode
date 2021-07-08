package com.atguigu.day03;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink02_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从socket读取数据源
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        // 将数据压平,并转化为javabean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0],
                        Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        // 将数据转为json，写入kafka
        SingleOutputStreamOperator<String> streamOperator = waterSensorDS.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor value) throws Exception {
                return JSON.toJSONString(value);
            }
        });

        // 将数据写入kafka
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        streamOperator.addSink(new FlinkKafkaProducer<String>("test", new SimpleStringSchema(),prop));


        env.execute(Flink02_Sink_Kafka.class.getName());
    }
}

package com.atguigu.day02;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class Flink05_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从kafka 读取数据
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG,"Flink_Test");
        DataStreamSource<String> kafkaDataSources = env.addSource(
                new FlinkKafkaConsumer<String>(
                        "topic_start", new SimpleStringSchema(), props));

        kafkaDataSources.print();

        // 执行
        env.execute(Flink05_Source_Kafka.class.getName());
    }
}

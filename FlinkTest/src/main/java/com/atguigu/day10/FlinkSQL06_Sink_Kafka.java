package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL06_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从端口读取数据，并转换为javaBean
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2])
                );
                return waterSensor;
            }
        });

        // 将流转化为 表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 查询数据
        Table select = sensorTable.where($("id").isEqual("ws_001"))
                .select($("id"), $("ts"), $("vc"));

        // 创建kafka连接器，讲数据写入kafka消费者
        tableEnv.connect(new Kafka()
                    .version("universal")
                    .topic("test")
                    .startFromLatest()
                    .sinkPartitionerRoundRobin()
                    .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withSchema(new Schema()
                    .field("id", DataTypes.STRING())
                    .field("ts",DataTypes.BIGINT())
                    .field("vc",DataTypes.INT()))
                .withFormat(new Json())
                .createTemporaryTable("sensor");

        // 写入到kafka
        select.executeInsert("sensor");

        env.execute(FlinkSQL06_Sink_Kafka.class.getName());
    }
}

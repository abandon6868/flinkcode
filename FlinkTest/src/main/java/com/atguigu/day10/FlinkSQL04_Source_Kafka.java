package com.atguigu.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从kafka 读取数据
        tableEnv.connect(new Kafka()
                // 版本支持类型："0.8", "0.9", "0.10", "0.11", and "universal"，但是1.12中只支持 universal
                .version("universal")
                .topic("test")
                .startFromLatest()
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG,"BigData"))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT()))
                .withFormat(new Json())  // kafka 支持的类型： Csv()  Json()
                // .withFormat(new Csv())
                .createTemporaryTable("sensor");

        // 将连接器作用于表
        Table sensor = tableEnv.from("sensor");

        // 查询数据
        Table select = sensor.groupBy($("id"))
                .select($("id"), $("id").count());

        // 将表转换为流进行输出
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(select, Row.class);
        tuple2DataStream.print();

        env.execute(FlinkSQL04_Source_Kafka.class.getName());
    }
}

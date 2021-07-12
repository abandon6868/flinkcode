package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL08_Sink_ES {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        // 从端口读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(
                        split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        Table table = envTable.fromDataStream(waterSensorDS);

        Table select = table.groupBy($("id"), $("vc"))
                .select($("id"),
                        $("ts").count().as("tc"),
                        $("vc"));

        // 使用es连接器，将数据写入es
        envTable.connect(new Elasticsearch()
                    .version("6")
                    .index("sensor_doc")
                    .documentType("_doc")
                    .host("hadoop102",9200,"http")
                    .bulkFlushMaxActions(1))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT()))
                .withFormat(new Json())
                .inUpsertMode()  // 使用Upsert 更新模式的话，id就是分组的信息拼接起来的
                .createTemporaryTable("sensor");

        select.executeInsert("sensor");

        env.execute();
    }
}

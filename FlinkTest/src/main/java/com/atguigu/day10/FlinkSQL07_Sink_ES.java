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

public class FlinkSQL07_Sink_ES {
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

        Table sensorTable = envTable.fromDataStream(waterSensorDS);

        Table select = sensorTable.groupBy($("id"))
                .select($("id"), $("ts").count(), $("vc").sum());

        // 使用es连接器，将数据写入es
        envTable.connect(new Elasticsearch()
                    .version("6")         // 版本
                    .host("hadoop102",9200,"http")  // 连接es的端口信息
                    .index("senser_Sql")   // index
                    .documentType("_doc")  // es 描述
                    .bulkFlushMaxActions(1))  // 批量写入的条数
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("vc",DataTypes.INT()))
                .withFormat(new Json())
                .inAppendMode()   // 使用写入的模式，追加模式在es中id是随机的
                .createTemporaryTable("sensor");

        select.executeInsert("sensor");

        env.execute();
    }
}

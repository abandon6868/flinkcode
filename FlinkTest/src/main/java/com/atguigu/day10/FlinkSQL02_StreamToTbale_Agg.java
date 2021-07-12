package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL02_StreamToTbale_Agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        // 将数据转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
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

        // 获取表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        // 查询数据，查询出 vc >= 20的数据，并计算 select id,sum(vc) from table where vc >=20 group by vc
        // 1.12 接口
        Table selectTable = sensorTable.where($("vc").isGreaterOrEqual(20))
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vc_sum"))
                .select($("id"), $("vc_sum"));

        // 老接口vc
        Table select = sensorTable
                .where("vc>=20")
                .groupBy("id")
                .select("id,vc.sum");

        // 将数据转换为流进行输出
        /*如果带了聚合操作，就不能用追加流了，因为追加流是不支持更新的，需要使用回撤流*/
        // DataStream<Row> rowDataStream = tableEnv.toAppendStream(selectTable, Row.class);
        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(select, Row.class);
        rowDataStream.print();

        env.execute();
    }
}

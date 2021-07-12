package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQL09_SQL_Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        // 从端口读取数据，并转为javaBean
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

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

        // 将流转换为动态表
        Table table = envTable.fromDataStream(waterSensorDS);

        // 使用sql查询未注册的表, 使用时注意table两边加空格
        // Table result = envTable.sqlQuery("select id,ts,vc from " + table + " where id='ws_001'");
        Table result = envTable.sqlQuery("select id,sum(vc) from " + table + " group by id");

        // 将表打印输出 普通查询追加流，聚合查询使用撤回流
        // envTable.toAppendStream(result, Row.class).print();
        envTable.toRetractStream(result,Row.class).print();
        env.execute();

    }
}

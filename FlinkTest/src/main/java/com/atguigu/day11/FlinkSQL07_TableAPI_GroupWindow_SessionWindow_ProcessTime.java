package com.atguigu.day11;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;


//Flink-窗口操作(GroupWindow  基于处理时间的会话窗口)

public class FlinkSQL07_TableAPI_GroupWindow_SessionWindow_ProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从端口读取数据，并转换每一行数据为JavaBean
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        // 将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());

        // 开会话窗口计算结果
        Table result = table.window(Session.withGap(lit(5).seconds()) // 会话窗口大小
                                    .on($("pt")) // 指定窗口字段
                                    .as("sc"))    // 窗口别名
                            .groupBy($("id"), $("sc"))
                            .select($("id"), $("id").count());

        // 输出结果 -- 支持追加流与撤回流，因为在窗口关闭时才进行计算
        tableEnv.toAppendStream(result, Row.class).print();

        env.execute();

    }
}

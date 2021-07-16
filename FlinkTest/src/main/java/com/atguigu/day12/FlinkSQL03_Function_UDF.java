package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

// 返回字符串长度
public class FlinkSQL03_Function_UDF {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                dataStreamSource.map(new MapFunction<String, WaterSensor>() {
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

        // 将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        // 使用自定义UTF函数
        // 方式一: 不注册函数，直接使用
        //table.select($("id"),call(StrLength.class,$("id"))).execute().print();

        // 方式二： 注册自定义函数  传入别名,全类名
        tableEnv.createTemporarySystemFunction("strLen",StrLength.class);
        // Table Api
        // table.select($("id"),call("strLen",$("id"))).execute().print();
        // Sql
        tableEnv.sqlQuery("select id,strLen(id) from " + table).execute().print();
        env.execute();

    }

    public static class StrLength extends ScalarFunction{
        public int eval(String val){
            return val.length();
        }
    }


}

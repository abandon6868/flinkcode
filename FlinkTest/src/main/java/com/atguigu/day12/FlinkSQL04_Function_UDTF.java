package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

// 对字符串切分，输出多个数据 -对多
public class FlinkSQL04_Function_UDTF {
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

        // 使用自定义UTTF函数

        // 注册UDTF
        tableEnv.createTemporarySystemFunction("strSplit",SplitStr.class);
        // Table API 用法
        /*table.joinLateral(call("strSplit",$("id")))
                .select($("id"),$("word"),$("len"))
                .execute()
                .print();*/
        // sql 用法
        tableEnv.sqlQuery("select id,word,len from " +
                table +
                " ,lateral table(strSplit(id))").
                execute().print();

    }

    /**
     * 对字符串切分，获取字符串与字符串长度,注意加注解
     */
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, len INT>"))
    public static class  SplitStr extends TableFunction<Row>{
        public void eval(String val){
            String[] split = val.split("_");
            for (String s : split) {
                collect(Row.of(s,s.length()));
            }
        }
    }


}

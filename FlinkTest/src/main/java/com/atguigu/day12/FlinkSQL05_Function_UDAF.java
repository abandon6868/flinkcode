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
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


// 求平均数，多对一
public class FlinkSQL05_Function_UDAF {
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

        // 使用自定义UTAF函数

        // 注册UDTF
        tableEnv.createTemporarySystemFunction("sensorAvg", WaterSensorAvg.class);
        // table API
        /*table.groupBy($("id"))
                .select($("id"), call("sensorAvg", $("vc")))
                .execute()
                .print();*/

        tableEnv.sqlQuery("select id,sensorAvg(vc) from " + table + " group by id")
                .execute()
                .print();

    }

    /**
     * 求平均数，多对一
     * 注意在使用UDAF自定义函数时,需要自己加累加器
     */
    public static class WaterSensorAvg extends AggregateFunction<Double, SumCount> {

        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }

        public void accumulate(SumCount acc, Integer vc) {
            acc.setSum(acc.getSum() + vc);
            acc.setCount(acc.getCount() + 1);
        }

        @Override
        public Double getValue(SumCount accumulator) {
            return accumulator.getSum() * 1D / accumulator.getCount();
        }

    }


}

package com.atguigu.day12;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;


// 按照id分组，求每个id的最高2位水位线
public class FlinkSQL05_Function_UDTAF {
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
        tableEnv.createTemporarySystemFunction("top2",Top2.class);
        // table API
        table.groupBy($("id"))
                .flatAggregate(call("top2",$("vc")).as("top","rank"))
                .select($("id"), $("top"),$("rank"))
                .execute()
                .print();


        env.execute();

    }

    /**
     * 求Top2
     * 注意在使用UDTAF自定义函数时,需要自己加累加器,与 发射函数
     */

    public static class Top2 extends TableAggregateFunction<Tuple2<Integer,String>,VcTop2>{
        // 因为水位线有负数，所以这里将默认值设置为最小值
        // 初始化
        @Override
        public VcTop2 createAccumulator() {
            return new VcTop2(Integer.MIN_VALUE,Integer.MIN_VALUE) ;
        }

        // 定义累加器
        public void accumulate(VcTop2 acc, Integer value) {
            if (value> acc.getTopOne()){
                acc.setTopTwo(acc.getTopOne());
                acc.setTopOne(value);

            }else if(value >acc.getTopTwo()){
                acc.setTopTwo(value);
            }
        }

        public void emitValue(VcTop2 acc, Collector<Tuple2<Integer, String>> out) {
            if (acc.getTopOne() != Integer.MIN_VALUE){
                out.collect(new Tuple2<>(acc.getTopOne(),"Top1"));
            }
            if (acc.getTopTwo() != Integer.MIN_VALUE){
                out.collect(new Tuple2<>(acc.getTopTwo(),"Top2"));
            }
        }
    }


}

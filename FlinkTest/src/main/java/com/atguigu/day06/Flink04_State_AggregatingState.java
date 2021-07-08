package com.atguigu.day06;

import com.atguigu.bean.AvgVC;
import com.atguigu.bean.WaterSensor;
import com.atguigu.bean.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// 计算每个传感器的平均水位
public class Flink04_State_AggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(
                new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor;
            }
        });

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(WaterSensor::getId);

        SingleOutputStreamOperator<WaterSensor2> streamOperator = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor2>() {
            private AggregatingState<Integer, Double> avgState;

            @Override
            public void open(Configuration parameters) throws Exception {
                avgState = getRuntimeContext().getAggregatingState(
                        new AggregatingStateDescriptor<Integer, AvgVC, Double>("avg-State", new AggregateFunction<Integer, AvgVC, Double>() {
                            @Override
                            public AvgVC createAccumulator() {
                                return new AvgVC(0L, 0L);
                            }

                            @Override
                            public AvgVC add(Integer value, AvgVC accumulator) {
                                return new AvgVC(accumulator.getVcSum() + value,
                                        accumulator.getVcCount() + 1);
                            }

                            @Override
                            public Double getResult(AvgVC accumulator) {
                                return (accumulator.getVcSum() / accumulator.getVcCount()) * 1D;
                            }

                            @Override
                            public AvgVC merge(AvgVC a, AvgVC b) {
                                return new AvgVC(a.getVcSum() + b.getVcSum(),
                                        a.getVcCount() + b.getVcCount());
                            }
                        }, AvgVC.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor2> out) throws Exception {
                // 将当前值更新值状态中
                avgState.add(value.getVc());
                // 取出状态数据
                Double avgVc = avgState.get();
                // 输出数据
                out.collect(new WaterSensor2(value.getId(), value.getTs(), avgVc));
            }
        });

        streamOperator.print();


        env.execute(Flink04_State_AggregatingState.class.getName());

    }
}

package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

// 水位线跳变
public class Flink02_State_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
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

        SingleOutputStreamOperator<String> flatMap = keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {
            // 定义状态
            private ValueState<Integer> vcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                // 初始化状态
                vcState = getRuntimeContext().getState(
                        new ValueStateDescriptor<Integer>("vc-State", Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
                // 从状态中获取值
                Integer lastVc = vcState.value();

                if (lastVc != null && Math.abs(lastVc - value.getVc()) > 10) {
                    out.collect(value.getId() + ":出现水位线跳变！");
                }
                vcState.update(value.getVc());

            }
        });

        flatMap.print();

        env.execute(Flink02_State_ValueState.class.getName());
    }
}

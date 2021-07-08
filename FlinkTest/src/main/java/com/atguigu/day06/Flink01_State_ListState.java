package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Flink01_State_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                streamSource.map(new MapFunction<String, WaterSensor>() {
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

        // 使用ListState 实现每隔传感器最高的三个水位线
        SingleOutputStreamOperator<List<WaterSensor>> flatMap = keyedStream.flatMap(new RichFlatMapFunction<WaterSensor, List<WaterSensor>>() {
            private ListState<WaterSensor> top3State;

            @Override
            public void open(Configuration parameters) throws Exception {
                top3State = getRuntimeContext().getListState(
                        new ListStateDescriptor<WaterSensor>("top3-State", WaterSensor.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<List<WaterSensor>> out) throws Exception {
                // 将当前值添加值状态中
                top3State.add(value);
                // 从状态值获取当前的数据
                ArrayList<WaterSensor> waterSensors = Lists.newArrayList(top3State.get().iterator());

                waterSensors.sort(new Comparator<WaterSensor>() {
                    @Override
                    public int compare(WaterSensor o1, WaterSensor o2) {
                        return o2.getVc() - o1.getVc();
                    }
                });

                // 判断当前数据是否超过三条,多余的数据移除
                if (waterSensors.size() > 3) {
                    waterSensors.remove(3);
                }
                // 更新状态
                top3State.update(waterSensors);
                // 返回值
                out.collect(waterSensors);
            }
        });

        flatMap.print();


        env.execute(Flink01_State_ListState.class.getName());
    }
}

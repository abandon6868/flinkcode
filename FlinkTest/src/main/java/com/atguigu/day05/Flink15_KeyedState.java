package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Flink15_KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource =
                env.socketTextStream("hadoop102", 9999);
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

        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new MyStateFunc());

        result.print();

        env.execute(Flink15_KeyedState.class.getName());
    }

    public static class MyStateFunc extends KeyedProcessFunction<String, WaterSensor, WaterSensor> {
        // a 定义状态  -- Flink 5种状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String,Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor,WaterSensor> aggregatingState;


        @Override
        public void open(Configuration parameters) throws Exception {
            // 状态初始化
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-State",Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-State",Long.class));
            mapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Long>("map-State",String.class,Long.class));
//            reducingState = getRuntimeContext().getState(new ReducingStateDescriptor<WaterSensor>("reducing-State", ))
//            aggregatingState = getRuntimeContext().getAggregatingState(
//                    new AggregatingStateDescriptor<WaterSensor, Object, WaterSensor>("aggregating-State",));
        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            // c 状态的使用
            // 1 value 的状态
            Long value1 = valueState.value(); // 获取value的值
            valueState.update(122L);  // 更新状态值

            // 2 ListState
            Iterable<Long> longs = listState.get();  // 获取value的值
            listState.add(122L);  // 向列表状态中添加值
            listState.clear();   // 清空列表状态值
            listState.update(new ArrayList<>()); // 更新整个状态值

            // 3 Map状态
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();  // 获取所有的状态值
            Long val = mapState.get("hello");  // 获取耨个key的状态值
            boolean b = mapState.contains("hello");  // 判断某个key是否存在
            mapState.put("world",122L);  // 向状态中添加某个值
            mapState.putAll(new HashMap<>()); // 更新整个状态值
            mapState.remove("hello");  // 从状态值中移除某个key
            mapState.keys();  // 获取所有的key
            mapState.clear();   // 清空整个状态值

            // 4 Reduce 状态
            reducingState.get(); // 获取状态值
            reducingState.add(new WaterSensor()); // 添加状态值
            reducingState.clear(); // 清空状态值

            // 5 Aggregate状态
            aggregatingState.add(value);
            WaterSensor waterSensor = aggregatingState.get();
            aggregatingState.clear();
        }
    }
}

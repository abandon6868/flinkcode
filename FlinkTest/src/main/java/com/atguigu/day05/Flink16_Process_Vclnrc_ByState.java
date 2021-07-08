package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink16_Process_Vclnrc_ByState {
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

        SingleOutputStreamOperator<WaterSensor> result =
                keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
            // 定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;

            // 初始化状态
            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-State", Integer.class));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-State", Long.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                // 取出当前水位线
                Integer curVc = value.getVc();

                // 判断数据是否是第一次输入,如果是第一次结果未NUll，不能做比较
                if (vcState.value() == null) {
                    vcState.update(Integer.MIN_VALUE);
                }

                // 取出状态数据
                Integer lastVC = vcState.value();
                Long timerTs = tsState.value();

                // 判断水位线是否下降
                if (curVc >= lastVC && timerTs == null) {
                    // 获取当前时间
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    // 注册定时器
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    // 更新时间状态
                    tsState.update(ts);
                } else if (curVc < lastVC && timerTs != null) {  // 删除定时器，如果连续下降，会删除多次同一个定时器，但同一个定时器第二次删除时为NULL
                    // 删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    // 清空状态
                    tsState.clear();
                }
                // 更新vc状态
                vcState.update(curVc);
                // 输出数据
                out.collect(value);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("SideOutPut") {
                           },
                        ctx.getCurrentKey() + "连续10秒没有下降！");
                tsState.clear();
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("SideOutPut"){}).print("SideOutPut");


        env.execute(Flink16_Process_Vclnrc_ByState.class.getName());
    }
}

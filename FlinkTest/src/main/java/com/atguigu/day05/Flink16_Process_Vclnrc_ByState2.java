package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink16_Process_Vclnrc_ByState2 {
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

        KeyedStream<WaterSensor, String> keyedStream = waterSensorDS.keyBy(data -> data.getId());

        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new WaterSenSorLnrc());

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("SideOutPut"){}).print("SideOutPut");

        env.execute(Flink16_Process_Vclnrc_ByState2.class.getName());
    }
    
    public static class WaterSenSorLnrc extends
            KeyedProcessFunction<String,WaterSensor,WaterSensor>{
        // 设置状态
        private ValueState<Integer> vcState;
        private ValueState<Long> tsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            vcState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Integer>("vc-State", Integer.class));
            tsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("ts-State",Long.class));
        }

        @Override
        public void processElement(WaterSensor value, 
                                   Context ctx, Collector<WaterSensor> out) throws Exception {
            // 获取当前的水位线
            Integer currVc = value.getVc();
            // 获取状态值
            Integer lastVc = vcState.value();
            Long timerTs = tsState.value();
            // 判断是否为第一次输入key
            if (lastVc == null){
                vcState.update(Integer.MIN_VALUE);
                lastVc = vcState.value();
            }

            // 触发定时器的条件，当前水位大于lastVc,timeTs为空
            if (currVc >= lastVc && timerTs == null){
                // 触发定时器
                Long ts = ctx.timerService().currentProcessingTime() + 1000L;
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新时间状态
                tsState.update(ts);
            }else if ( currVc < lastVc && timerTs != null ){ // 删除定时器
                ctx.timerService().deleteProcessingTimeTimer(timerTs);
                // 清空状态
                tsState.clear();
            }

            vcState.update(currVc);
            out.collect(value);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx,
                            Collector<WaterSensor> out) throws Exception {
            ctx.output(new OutputTag<String>("SideOutPut"){},
                    ctx.getCurrentKey() + "连续10秒没有下降！");
        }
    }
}

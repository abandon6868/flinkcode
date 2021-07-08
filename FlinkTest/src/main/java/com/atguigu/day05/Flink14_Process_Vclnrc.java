package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink14_Process_Vclnrc {
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

        // 使用ProcessFunction实现连续时间内水位不下降,则报警,且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = keyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private Integer lastVc = Integer.MIN_VALUE;
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out)
                    throws Exception {
                // 当前水位
                Integer vc = value.getVc();
                System.out.println("当前水位" + vc);

                if (vc >= lastVc && timer == Long.MIN_VALUE) {
                    // 注册定时器 10s
                    System.out.println("注册定时器");
                    // 获取当前时间
                    long currTs = ctx.timerService().currentProcessingTime()+ 10000L;
                    ctx.timerService().registerProcessingTimeTimer(currTs);
                    // 更新
                    timer = currTs;
                } else if (vc < lastVc) {
                    // 触发定时器
                    System.out.println("删除定时器:" + timer);
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    // 更新定时器
                    timer = Long.MIN_VALUE;

                }
                lastVc = vc;

                // 返回结果
                out.collect(value);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out)
                    throws Exception {
                // 定时器触发
                ctx.output(new OutputTag<String>("sideOut"){},
                        ctx.getCurrentKey() + "连续10s水位线未下降");

                timer = Long.MIN_VALUE;
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOut"){}).print("sideOutPut");


        env.execute();
    }
}

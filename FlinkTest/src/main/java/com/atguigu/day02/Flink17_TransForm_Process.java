package com.atguigu.day02;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink17_TransForm_Process {
    public static void main(String[] args) throws Exception {
        // 1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2 获取输入流
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        // 3 使用process算子替代别的flatMap计算wordCount
        SingleOutputStreamOperator<String> wordDS = streamSource.process(new MyProcessFlatMap());

        // 4 使用process算子替代map算子
        SingleOutputStreamOperator<Tuple2<String, Integer>> word2Ones = wordDS.process(new MyProcessMapFunc());

        // 5 对key进行分组
        KeyedStream<Tuple2<String, Integer>, String> stringKeyedStream = word2Ones.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        // 6 计算求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = stringKeyedStream.sum(1);

        // 7 输出结果
        result.print();

        // 8 开始执行
        env.execute(Flink17_TransForm_Process.class.getName());

    }

    private static class MyProcessFlatMap extends ProcessFunction<String,String> {

        // 生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {

        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
            // 运行上下文，做状态编程
            RuntimeContext runtimeContext = getRuntimeContext();
            String[] split = value.split(" ");
            for (String s : split) {
                out.collect(s);
            }

            // 定时器
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(1234L);

            // 获取当前处理的时间
            long processingTime = timerService.currentProcessingTime();
            // 事件事件
            long currentWatermark = timerService.currentWatermark();

            // 侧输出流
            //  ctx.output();

        }
    }

    private static class MyProcessMapFunc extends  ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(
                String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }
}

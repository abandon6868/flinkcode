package com.atguigu.day08;

import com.atguigu.bean.LoginEvent;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * 连续两次失败判断
 */
public class Flink05_Practice_LoginApp2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\LoginLog.csv");

        // 转换为javaBean，并生成waterMark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                LoginEvent loginEvent = new LoginEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
                return loginEvent;
            }
        });
        WatermarkStrategy<LoginEvent> watermarkStrategy =
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(
                        Duration.ofSeconds(10)).withTimestampAssigner(
                        new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        });

        SingleOutputStreamOperator<LoginEvent> singleOutputStreamOperator =
                loginEventDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照用户id进行分组
        KeyedStream<LoginEvent, Long> keyedStream =
                singleOutputStreamOperator.keyBy(LoginEvent::getUserId);

        // 使用ProcessApi状态+定时器
        SingleOutputStreamOperator<String> result =
                keyedStream.process(new LoginKeyedProcessFunc(2));

        result.print();

        env.execute(Flink05_Practice_LoginApp2.class.getName());
    }

    private static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
        private Integer ts;
        private ValueState<LoginEvent> failEventState;

        public LoginKeyedProcessFunc(Integer ts) {
            this.ts = ts;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            failEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<LoginEvent>("fail-EventState",LoginEvent.class));
        }

        @Override
        public void processElement(LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            //判断数据类型
            if ("fail".equals(value.getEventType())){
                // 从状态中取出数据
                LoginEvent loginEvent = failEventState.value();

                failEventState.update(value);

                 // 不是第一次登录，并且两次失败时间间隔小于等于ts
                    if (loginEvent != null && Math.abs(value.getEventTime() - loginEvent.getEventTime()) <= ts){
                        //输出报警信息
                        out.collect(value.getUserId() + "连续登录失败2次！");

                    }
                } else {
                //成功数据,清空状态
                failEventState.clear();
            }
        }
    }
}

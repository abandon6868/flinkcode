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

public class Flink04_Practice_LoginApp {
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
        WatermarkStrategy<LoginEvent> watermarkStrategy = WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
                new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                });

        SingleOutputStreamOperator<LoginEvent> singleOutputStreamOperator = loginEventDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照用户id进行分组
        KeyedStream<LoginEvent, Long> keyedStream = singleOutputStreamOperator.keyBy(LoginEvent::getUserId);

        // 使用ProcessApi状态+定时器
        SingleOutputStreamOperator<String> result = keyedStream.process(new LoginKeyedProcessFunc(2, 2));

        result.print();

        env.execute(Flink04_Practice_LoginApp.class.getName());
    }

    private static class LoginKeyedProcessFunc extends KeyedProcessFunction<Long, LoginEvent, String> {
        private Integer ts;
        private Integer count;

        // 定义状态
        private ListState<LoginEvent> loginEventListState;
        private ValueState<Long> valueState;

        public LoginKeyedProcessFunc(Integer ts, Integer count) {
            this.ts = ts;
            this.count = count;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            loginEventListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<LoginEvent>("login-EventListState",LoginEvent.class));
            valueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("value-State",Long.class));
        }

        @Override
        public void processElement(
                LoginEvent value, Context ctx, Collector<String> out) throws Exception {
            // 从状态中获取数据
            Iterator<LoginEvent> loginEvents = loginEventListState.get().iterator();
            Long timerTs  = valueState.value();

            String eventType = value.getEventType();
            // 判断否为第一条失败数据,则需要注册定时器
            if ("fail".equals(eventType)){
                if (!loginEvents.hasNext()){
                    // 注册定时器
                    long currTs = ctx.timerService().currentWatermark() + ts * 1000L;
                    ctx.timerService().registerEventTimeTimer(currTs);
                }
                //将当前的失败数据加入状态
                loginEventListState.add(value);
            }else {
                //说明已经注册过定时器,删除定时器
                if (timerTs != null){
                    ctx.timerService().deleteEventTimeTimer(timerTs);
                }
                loginEventListState.clear();
                valueState.clear();
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            Iterator<LoginEvent> iterator = loginEventListState.get().iterator();
            ArrayList<LoginEvent> loginEvents = Lists.newArrayList(iterator);
            //判断连续失败的次数
            if (loginEvents.size() >= count){
                LoginEvent first = loginEvents.get(0);
                LoginEvent last = loginEvents.get(loginEvents.size() - 1);

                out.collect(first.getUserId() +
                        "用户在" +
                        first.getEventTime() +
                        "到" +
                        last.getEventTime() +
                        "之间，连续登录失败了" +
                        loginEvents.size() + "次！");
            }
            //清空状态
            loginEventListState.clear();
            valueState.clear();

        }
    }
}

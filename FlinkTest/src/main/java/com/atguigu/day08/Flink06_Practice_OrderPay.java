package com.atguigu.day08;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.time.Duration;

/**
 * 订单创建后，15Min后没支付，为一个超时订单
 * 实时对账
 */

public class Flink06_Practice_OrderPay {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("F:\\myFlink\\FlinkTest\\input\\OrderLog.csv");
        // 将数据转换为javaBean，并提取waterMark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = dataStreamSource.map(
                new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
                return orderEvent;
            }
        });

        WatermarkStrategy<OrderEvent> watermarkStrategy =
                WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });

        SingleOutputStreamOperator<OrderEvent> timestampsAndWatermarks =
                orderEventDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照订单id分组
        KeyedStream<OrderEvent, Long> keyedStream = timestampsAndWatermarks.keyBy(OrderEvent::getOrderId);

        //使用状态编程+定时器实现订单的获取
        SingleOutputStreamOperator<String> result = keyedStream.process(new OrderPayProcessFunc(15));

        result.print();
        result.getSideOutput(new OutputTag<String>(" Payed TimeOut Or No Create"){}).print("No Create");
        result.getSideOutput(new OutputTag<String>("No Pay"){}).print("No Pay");


        env.execute(Flink06_Practice_OrderPay.class.getName());
    }

    private static class OrderPayProcessFunc extends KeyedProcessFunction<Long,OrderEvent,String> {
        private ValueState<OrderEvent> eventValueState;
        private ValueState<Long> timerOnState;
        private Integer interval;

        public OrderPayProcessFunc(Integer interval) {
            this.interval = interval;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            eventValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("event-ValueState",OrderEvent.class));
            timerOnState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timerOn-State",Long.class));
        }

        @Override
        public void processElement(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
            // 判断当前的数据中的类型
            if ("create".equals(value.getEventType())){
                eventValueState.update(value);
                // 注册定时器 interval
                Long ts = value.getEventTime() + interval * 60;
                ctx.timerService().registerEventTimeTimer(ts);
                timerOnState.update(ts);
            }else if ("pay".equals(value.getEventType())){
                // 从状态中取出数据
                OrderEvent orderEvent = eventValueState.value();
                if (orderEvent == null){
                    // 丢失了创建数据,或者超过15分钟才支付
                    ctx.output(new OutputTag<String>(" Payed TimeOut Or No Create"){},
                            value.getOrderId() + " Payed But No Create！");
                }else {
                    // 输出数据
                    out.collect(value.getOrderId() +
                            " Create at " +
                            value.getEventTime() +
                            " Payed at " + value.getEventTime());
                    // 清空状态，删除定时器
                    ctx.timerService().deleteEventTimeTimer(timerOnState.value());
                    eventValueState.clear();
                    timerOnState.clear();
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //取出状态中的数据
            OrderEvent orderEvent = eventValueState.value();

            ctx.output(new OutputTag<String>("No Pay"){},orderEvent.getOrderId() + " Create But No Pay!");
            //清空状态
            eventValueState.clear();
            timerOnState.clear();
        }
    }
}

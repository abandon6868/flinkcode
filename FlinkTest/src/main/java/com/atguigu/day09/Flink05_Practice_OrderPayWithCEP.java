package com.atguigu.day09;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * 订单创建后，15Min后没支付，为一个超时订单
 * 实时对账
 */

public class Flink05_Practice_OrderPayWithCEP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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

        // 定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).followedBy("end").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        // 将模式作用于流上
        PatternStream<OrderEvent> eventPatternStream = CEP.pattern(keyedStream, pattern);

        // 提取匹配的事件
        SingleOutputStreamOperator<String> result = eventPatternStream.select(new OutputTag<String>("NO Pay") {
                                                                              },
                new OrderPayTimeOutFunc(),
                new OrderPaySelectFunc());

        result.print();
        result.getSideOutput(new OutputTag<String>("NO Pay") {}).print("Time Out");

        env.execute(Flink05_Practice_OrderPayWithCEP.class.getName());
    }


    private static class OrderPayTimeOutFunc implements PatternTimeoutFunction<OrderEvent,String> {
        @Override
        public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
            OrderEvent orderEvent = pattern.get("start").get(0);
            return orderEvent.getOrderId() + "在 " + orderEvent.getEventTime() +
                    " 创建订单，并在 " + timeoutTimestamp / 1000L + " 超时";
        }
    }

    private static class OrderPaySelectFunc implements PatternSelectFunction<OrderEvent,String> {
        @Override
        public String select(Map<String, List<OrderEvent>> pattern) throws Exception {
            OrderEvent orderEvent = pattern.get("start").get(0);
            OrderEvent orderEvent1 = pattern.get("end").get(0);
            return orderEvent.getOrderId() + " 在 " + orderEvent.getEventTime() + "创建订单,并在 "
                    + orderEvent1.getEventTime() + "支付成功！！！";
        }
    }
}

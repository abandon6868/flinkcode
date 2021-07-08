package com.atguigu.day09;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink01_Practice_OrderReceiptWithState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据
        DataStreamSource<String> orderDataStreamSource = env.readTextFile("F:\\myFlink\\FlinkTest\\input\\OrderLog.csv");
        DataStreamSource<String> receiptDataStreamSource = env.readTextFile("F:\\myFlink\\FlinkTest\\input\\ReceiptLog.csv");

        // 将数据保存为JavaBean，并提取waterMark
        WatermarkStrategy<OrderEvent> orderEventWatermarkStrategy = WatermarkStrategy.<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });
        WatermarkStrategy<TxEvent> txEventWatermarkStrategy = WatermarkStrategy.<TxEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<TxEvent>() {
            @Override
            public long extractTimestamp(TxEvent element, long recordTimestamp) {
                return element.getEventTime() * 1000L;
            }
        });

        SingleOutputStreamOperator<OrderEvent> orderEventDS = orderDataStreamSource.flatMap(new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] split = value.split(",");
                OrderEvent orderEvent = new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
                if ("pay".equals(orderEvent.getEventType())){
                    out.collect(orderEvent);
                }
            }
        }).assignTimestampsAndWatermarks(orderEventWatermarkStrategy);

        SingleOutputStreamOperator<TxEvent> txEventDS = receiptDataStreamSource.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                TxEvent txEvent = new TxEvent(
                        split[0],
                        split[1],
                        Long.parseLong(split[2])
                );
                return txEvent;
            }
        }).assignTimestampsAndWatermarks(txEventWatermarkStrategy);

        // 对两个流进行连接,然后进行分组
        ConnectedStreams<OrderEvent, TxEvent> connectedStreams = orderEventDS.connect(txEventDS).keyBy("txId", "txId");

        // 使用状态，计算
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result =
                connectedStreams.process(new ReceiptKeyedCoProcessFunc());

        result.print();
        result.getSideOutput(new OutputTag<String>("Receipt No Payed"){}).print("No Payed");;
        result.getSideOutput(new OutputTag<String>("Payed No Receipt"){}).print("No Receipt");;


        env.execute(Flink01_Practice_OrderReceiptWithState.class.getName());
    }

    private static class ReceiptKeyedCoProcessFunc extends KeyedCoProcessFunction<String,OrderEvent,TxEvent,Tuple2<OrderEvent, TxEvent>>{
        private ValueState<OrderEvent> payEventValueState;
        private ValueState<TxEvent> txEventValueState;
        private ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            payEventValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<OrderEvent>("order_EventValueState",OrderEvent.class));
            txEventValueState = getRuntimeContext().getState(
                    new ValueStateDescriptor<TxEvent>("tx_EventValueState",TxEvent.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs_State",Long.class));
        }

        @Override
        public void processElement1(
                OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出对账状态数据
            TxEvent txEvent = txEventValueState.value();
            //判断到账数据是否已经到达
            if (txEvent == null){ // 数据未到达
                // 将订单数据添加进状态
                payEventValueState.update(value);
                // 注册定时器
                Long ts = (value.getEventTime() +10) * 1000L;
                ctx.timerService().registerEventTimeTimer(ts);
                timerTsState.update(ts);

            }else { // 数据达到
                // 输出数据
                out.collect(new Tuple2<>(value,txEvent));
                // 清空状态，删除定时器
                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
//                payEventValueState.clear();
                txEventValueState.clear();
                timerTsState.clear();
            }
        }

        @Override
        public void processElement2(
                TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出订单状态数据
            OrderEvent orderEvent = payEventValueState.value();
            // 判断支付数据
            if (orderEvent == null){ // 支付数据为到达
                // 添加状态，注册定时器
                txEventValueState.update(value);

               Long ts =  (value.getEventTime() +5)*1000L;
               ctx.timerService().registerEventTimeTimer(ts);
               timerTsState.update(ts);

            }else { // 支付数据到达
                // 输出数据，并清空状态，删除定时器
                out.collect(new Tuple2<>(orderEvent,value));

                ctx.timerService().deleteEventTimeTimer(timerTsState.value());
                payEventValueState.clear();
                // txEventValueState.clear();
                timerTsState.clear();

            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<OrderEvent, TxEvent>> out) throws Exception {
            // 取出状态的数据
            OrderEvent orderEvent = payEventValueState.value();
            TxEvent txEvent = txEventValueState.value();

            //判断orderEvent是否为Null
            if (orderEvent != null) {
                ctx.output(new OutputTag<String>("Payed No Receipt") {
                           },
                        orderEvent.getTxId() + "只有支付没有到账数据");
            } else {
                ctx.output(new OutputTag<String>("Receipt No Payed") {
                           },
                        txEvent.getTxId() + "只有到账没有支付数据");
            }

            // 清空状态
            payEventValueState.clear();
            txEventValueState.clear();
            timerTsState.clear();
        }
    }
}

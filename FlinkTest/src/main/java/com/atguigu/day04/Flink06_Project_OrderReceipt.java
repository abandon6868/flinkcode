package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.stream.Stream;

public class Flink06_Project_OrderReceipt {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中获取数据流
        DataStreamSource<String> txEventDS = env.readTextFile("input/ReceiptLog.csv");
        DataStreamSource<String> orderEventDS = env.readTextFile("input/OrderLog.csv");

        // 3 对两个流做处理
        // a 处理 txEventDS 流
        SingleOutputStreamOperator<TxEvent> txEventStream = txEventDS.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] infos = value.split(",");
                return new TxEvent(infos[0],
                        infos[1],
                        Long.parseLong(infos[2]));
            }
        });
        // b 处理orderEventDS流
        SingleOutputStreamOperator<OrderEvent> orderEventStream = orderEventDS.flatMap(
                new FlatMapFunction<String, OrderEvent>() {
            @Override
            public void flatMap(String value, Collector<OrderEvent> out) throws Exception {
                String[] infos = value.split(",");
                OrderEvent orderEvent = new OrderEvent(Long.parseLong(infos[0]),
                        infos[1],
                        infos[2],
                        Long.parseLong(infos[3]));
                if ("pay".equals(orderEvent.getEventType())){
                    out.collect(orderEvent);
                }
            }
        });

        // 4 对两个流做分组
        KeyedStream<TxEvent, String> txEventKeyedStream = txEventStream.keyBy(data -> data.getTxId());
        KeyedStream<OrderEvent, String> orderEventKeyedStream = orderEventStream.keyBy(data -> data.getTxId());

        // 5 连接两个流
        ConnectedStreams<TxEvent, OrderEvent> connectedStreams = txEventKeyedStream.connect(orderEventKeyedStream);

        // 6 对两个流做处理
        SingleOutputStreamOperator<Tuple2<OrderEvent, TxEvent>> result = connectedStreams.process(new OrderKeyedCoProcessFunc());


        // 7 打印结果
        result.print();

        // 8 执行
        env.execute(Flink06_Project_OrderReceipt.class.getName());
    }

    public static class  OrderKeyedCoProcessFunc  extends
            KeyedCoProcessFunction<String, TxEvent, OrderEvent, Tuple2<OrderEvent,TxEvent>>{

        HashMap<String,TxEvent> txEventHashMap = new HashMap<>();
        HashMap<String,OrderEvent> orderHashMap = new HashMap<>();


        @Override
        public void processElement1(TxEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out)
                throws Exception {
            // value 为TxEvent 类型，
            if (orderHashMap.containsKey(value.getTxId())){
                OrderEvent orderEvent = orderHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(orderEvent,value));
            }else{
                txEventHashMap.put(value.getTxId(),value);
            }
        }

        @Override
        public void processElement2(OrderEvent value, Context ctx, Collector<Tuple2<OrderEvent, TxEvent>> out)
                throws Exception {

            if (txEventHashMap.containsKey(value.getTxId())){
                TxEvent txEvent = txEventHashMap.get(value.getTxId());
                out.collect(new Tuple2<>(value,txEvent));
            }else {
                orderHashMap.put(value.getTxId(),value);
            }

        }
    }

}

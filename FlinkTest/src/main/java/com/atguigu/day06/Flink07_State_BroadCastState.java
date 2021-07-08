package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink07_State_BroadCastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        // 读取数据
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        DataStreamSource<String> dataStreamSource1 = env.socketTextStream("hadoop102", 8888);

        // 将8888端口的数据广播出去,创建一个广播流
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, String.class);
        BroadcastStream<String> broadcastStream = dataStreamSource1.broadcast(
                mapStateDescriptor);

        // 将两个流进行连接
        BroadcastConnectedStream<String, String> connectedStream = dataStreamSource.connect(broadcastStream);

        // 对流做处理
        SingleOutputStreamOperator<String> streamOperator = connectedStream.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 从广播状态中取值, 不同的值做不同的业务
                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(mapStateDescriptor);
                if ("1".equals(state.get("sWitch"))) {
                    out.collect("切换到1号配置....");
                } else if ("2".equals(state.get("sWitch"))) {
                    out.collect("切换到2号配置....");
                } else {
                    out.collect("切换到其他配置....");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                BroadcastState<String, String> state = ctx.getBroadcastState(mapStateDescriptor);
                // 把值放入广播状态
                state.put("sWitch", value);
            }
        });

        streamOperator.print();


        env.execute(Flink07_State_BroadCastState.class.getName());
    }
}

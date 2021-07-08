package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_TransForm_Repatition {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从端口获取数据
        DataStreamSource<String> streamSource = env.socketTextStream(
                "hadoop102", 9999);

        // 使用不同的重分区策略分区打印结果
        streamSource.keyBy(x->x).print("KeyBy");
        streamSource.shuffle().print("Shuffle");
        streamSource.rebalance().print("Rebalance");
        streamSource.rescale().print("Rescale");
        streamSource.global().print("Global");
//        streamSource.broadcast().print("BroadCast"); // 每个slot都会分发一次
//        streamSource.forward().print("ForWard"); // 使用时要求并行度必须相同

        env.execute(Flink01_TransForm_Repatition.class.getName());
    }
}

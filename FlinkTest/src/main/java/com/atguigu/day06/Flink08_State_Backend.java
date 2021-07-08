package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_State_Backend {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 使用状态后端需要配合checkpoint使用
        env.getCheckpointConfig().enableUnalignedCheckpoints(); // 开启ck

        // 使用状态后端
        env.setStateBackend(new MemoryStateBackend());  // MemoryStateBackend
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:9000/ck"));   // FsStateBackend
        env.setStateBackend(new RocksDBStateBackend("hdfs:hadoop102:9999/ck")); // RocksDBStateBackend

        env.execute(Flink08_State_Backend.class.getName());
    }
}

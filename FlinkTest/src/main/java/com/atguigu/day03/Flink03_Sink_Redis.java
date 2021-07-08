package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

public class Flink03_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.flatMap(new FlatMapFunction<String, WaterSensor>() {
            @Override
            public void flatMap(String value, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0],
                        Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });

        // 将数据写入redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build();
        waterSensorDS.addSink(new RedisSink<>(jedisPoolConfig,new MyRedisMapper()));


        env.execute(Flink03_Sink_Redis.class.getName());


    }
    public static class MyRedisMapper implements RedisMapper<WaterSensor> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 只有hash情况下，需要设置 additionalKey
            return new RedisCommandDescription(RedisCommand.HSET,"waterSensor");
        }

        @Override
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        @Override
        public String getValueFromData(WaterSensor data) {
            return data.getVc().toString();
        }
    }
}

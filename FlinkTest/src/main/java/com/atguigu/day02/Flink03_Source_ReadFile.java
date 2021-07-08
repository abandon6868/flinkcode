package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_Source_ReadFile {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2 从文件中读取数据
//        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
        DataStreamSource<String> streamSource = env.readTextFile("hdfs://hadoop102:9000/input/sensor.txt");
        //3 将数据转换为JAVABean对象
        SingleOutputStreamOperator<WaterSensor> outputStreamOperator = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] infos = value.split(",");
                return new WaterSensor(infos[0], Long.parseLong(infos[1]), Integer.parseInt(infos[2]));
            }
        });

        outputStreamOperator.print();

        env.execute(Flink03_Source_ReadFile.class.getName());
    }
}

/**  如果需要读取hdfs中的数据，需要加上此配置
 <dependency>
     <groupId>org.apache.hadoop</groupId>
     <artifactId>hadoop-client</artifactId>
     <version>3.1.3</version>
     <scope>provided</scope>
 </dependency>
 */
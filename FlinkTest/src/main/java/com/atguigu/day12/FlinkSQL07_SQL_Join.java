package com.atguigu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


public class FlinkSQL07_SQL_Join {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从2个端口读取数据
        DataStreamSource<String> tableAStream = env.socketTextStream("hadoop102", 8888);
        DataStreamSource<String> tableBStream = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<TableA> tableADS =  tableAStream.map(new MapFunction<String, TableA>() {
            @Override
            public TableA map(String value) throws Exception {
                String[] split = value.split(",");
                return new TableA(Integer.parseInt(split[0]),
                        split[1]);
            }
        });

        SingleOutputStreamOperator<TableB> tableBDS = tableBStream.map(new MapFunction<String, TableB>() {
            @Override
            public TableB map(String value) throws Exception {
                String[] split = value.split(",");
                return new TableB(Integer.parseInt(split[0]),
                        Integer.parseInt(split[1]));
            }
        });

        // 将流转换为动态表表
        tableEnv.createTemporaryView("tableA",tableADS);
        tableEnv.createTemporaryView("tableB",tableBDS);

        // 表连接
        // 内连接
        tableEnv.sqlQuery("select * from tableA join tableB ON tableA.id = tableB.id")
                .execute().print();

        // 左连接，如果是左边数据先到 不管左边又没有匹配到数据，都会输出，没有的置为null，等右边数据到达，删除上个状态，使用最新状态数据
        //       如果是右边数据先到 没有输出，当左边数据到达才会输出
        // 如果单独设置了下面状态配置，当为 0 时：永远状态永久保存，
        // 不为 0 时，当到达这个时间点：就删除状态

        /*// 老版本设置,设置时间间隔最少为5分钟以上
        tableEnv.getConfig().getMaxIdleStateRetentionTime();  // 状态保存最大时间
        tableEnv.getConfig().getMinIdleStateRetentionTime();  // 状态保存最小时间*/
        //新版本 FLinkSQL状态保留10秒，距离最后一次使用状态 10s内没被使用的话，就删除状态
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        tableEnv.sqlQuery("select * from tableA left join tableB ON tableA.id = tableB.id").execute().print();

        env.execute();
    }
}

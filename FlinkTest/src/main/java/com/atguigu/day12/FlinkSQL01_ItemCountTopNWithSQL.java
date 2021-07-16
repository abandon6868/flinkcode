package com.atguigu.day12;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


public class FlinkSQL01_ItemCountTopNWithSQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 从文本读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorDS =
                streamSource.map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] split = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4]));
                        return userBehavior;

                    }
                }).filter(new FilterFunction<UserBehavior>() {
                            @Override
                            public boolean filter(UserBehavior value) throws Exception {
                                return "pv".equals(value.getBehavior());
                            }
                        });

        // 提取时间watermark
        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000L;
            }
        });
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleDS = userBehaviorDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 将userBehaviorDS流转换为动态表并指定事件时间字段
        Table table = tableEnv.fromDataStream(userBehaviorSingleDS,
                $("UserId"),
                $("itemId"),
                $("categoryId"),
                $("behavior"),
                $("timestamp"),
                $("rt").rowtime());

        // 使用Flink Sql Over Window 取TopN

        Table windowItemCountTable = tableEnv.sqlQuery("select " +
                " itemId," +
                " count(itemId) as ct," +
                " HOP_END(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR) as windowEnd " +
                " from " + table +
                " group by itemId,HOP(rt, INTERVAL '5' MINUTE, INTERVAL '1' HOUR)");
        // 按照窗口进行分组，排序

        Table rankTable = tableEnv.sqlQuery("select " +
                " itemId," +
                " ct," +
                " windowEnd," +
                " row_number() over(partition by windowEnd order by ct desc) as rk" +
                " from " + windowItemCountTable);
        Table result = tableEnv.sqlQuery("select * from " + rankTable + " where rk<=5");
        result.execute().print();
        env.execute();

    }
}

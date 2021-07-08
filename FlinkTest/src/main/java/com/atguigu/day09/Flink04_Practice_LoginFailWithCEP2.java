package com.atguigu.day09;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * 连续两次失败判断
 */
public class Flink04_Practice_LoginFailWithCEP2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\LoginLog.csv");

        // 转换为javaBean，并生成waterMark
        SingleOutputStreamOperator<LoginEvent> loginEventDS = streamSource.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                LoginEvent loginEvent = new LoginEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3])
                );
                return loginEvent;
            }
        });
        WatermarkStrategy<LoginEvent> watermarkStrategy =
                WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(
                        Duration.ofSeconds(10)).withTimestampAssigner(
                        new SerializableTimestampAssigner<LoginEvent>() {
                            @Override
                            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                                return element.getEventTime() * 1000L;
                            }
                        });

        SingleOutputStreamOperator<LoginEvent> singleOutputStreamOperator =
                loginEventDS.assignTimestampsAndWatermarks(watermarkStrategy);

        // 按照用户id进行分组
        KeyedStream<LoginEvent, Long> keyedStream =
                singleOutputStreamOperator.keyBy(LoginEvent::getUserId);

        // 定义模式序列
        /*Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) {
                return "fail".equals(value.getEventType());
            }
        }).next("next").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        }).within(Time.seconds(5));*/

        // 使用量词
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        })
                .times(2) // 默认使用的是宽松模式，会进行贪婪匹配
                .consecutive() // 指定使用严格近邻模式
                .within(Time.seconds(5));

        // 将模式作用于流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        // 提取匹配的事件
        SingleOutputStreamOperator<String> result = patternStream.select(new LoginFailPatternSelectFunc());

        result.print();

        env.execute(Flink04_Practice_LoginFailWithCEP2.class.getName());
    }

    private static class LoginFailPatternSelectFunc implements PatternSelectFunction<LoginEvent,String> {
        @Override
        public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
            // 提取数据
            LoginEvent start = pattern.get("start").get(0);
            LoginEvent next = pattern.get("start").get(1);
            // 输出数据
            return start.getUserId() + "在 " + start.getEventTime()
                    + " 到 " + next.getEventTime() + " 之间连续登录失败2次！";
        }
    }
}

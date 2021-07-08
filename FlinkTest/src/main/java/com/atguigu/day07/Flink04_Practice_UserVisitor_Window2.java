package com.atguigu.day07;

import com.atguigu.bean.UserBehavior;
import com.atguigu.bean.UserVisitorCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.sql.Timestamp;

public class Flink04_Practice_UserVisitor_Window2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从文本中读取数据
        DataStreamSource<String> streamSource =
                env.readTextFile("F:\\myFlink\\FlinkTest\\input\\UserBehavior.csv");

        // 将数据转换成map,并过滤数据
        SingleOutputStreamOperator<UserBehavior> userBehaviorFilterDS = streamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                UserBehavior userBehavior = new UserBehavior(Long.parseLong(split[0]),
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

        // 提取时间，生成watermark
        WatermarkStrategy<UserBehavior> userBehaviorWatermarkStrategy = WatermarkStrategy.<UserBehavior>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                        return element.getTimestamp() * 1000L;
                    }
                });
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator =
                userBehaviorFilterDS.assignTimestampsAndWatermarks(userBehaviorWatermarkStrategy);

        // 对用户行为进行分组
        KeyedStream<UserBehavior, String> behaviorStringKeyedStream =
                userBehaviorSingleOutputStreamOperator.keyBy(UserBehavior::getBehavior);

        // 开窗
        WindowedStream<UserBehavior, String, TimeWindow> behaviorStringTimeWindowWindowedStream =
                behaviorStringKeyedStream.window(TumblingEventTimeWindows.of(Time.hours(1)));

        // 使用布隆过滤器  自定义触发器:来一条计算一条(访问Redis一次)
        SingleOutputStreamOperator<UserVisitorCount> result = behaviorStringTimeWindowWindowedStream
                .trigger(new UserVisitTrigger())  // 自定义触发器，如果不使用自定义触发器，将会导致一条数据打印一次
                .process(new UserVisitorWindowFunc());

        result.print();
        env.execute(Flink04_Practice_UserVisitor_Window2.class.getName());
    }

    private static class UserVisitorWindowFunc extends
            ProcessWindowFunction<UserBehavior, UserVisitorCount,String,TimeWindow> {
        // 声明redis连接
        private Jedis jedis;
        // 声明布隆过滤器
        private UserVisitBloomFilter userVisitBloomFilter;
        // 声明每隔窗口总人数的key
        private String hourUvCountKey;

        // 初始化


        @Override
        public void open(Configuration parameters) throws Exception {
            jedis = new Jedis("localhost",6379);
            hourUvCountKey = "HourUv";
            // 1<<30 位运算，即2得30次方
            userVisitBloomFilter = new UserVisitBloomFilter(1L<<30L);
        }

        @Override
        public void process(String s, Context context, Iterable<UserBehavior>
                elements, Collector<UserVisitorCount> out) throws Exception {
            // 取出数据
            UserBehavior userBehavior = elements.iterator().next();

            // 提取窗口信息
            String windowEnd = new Timestamp(context.window().getEnd()).toString();

            // 定义当前窗口得BitMap key
            String bitMapKey = "BitMap_" + windowEnd;

            // 查询当前的UID中是否已经存在于当前的bitMap中
            Long bloomFilterOffSet = userVisitBloomFilter.getOffSet(userBehavior.getUserId().toString());
            Boolean aBoolean = jedis.getbit(bitMapKey, bloomFilterOffSet);


            if (!aBoolean){
                // 将对应的offset设置为1
                jedis.setbit(bitMapKey,bloomFilterOffSet,true);

                // 累积当前窗口得总和
                jedis.hincrBy(hourUvCountKey,windowEnd,1);
            }

            // 输出数据
            String hget = jedis.hget(hourUvCountKey, windowEnd);
            out.collect(new UserVisitorCount("UV",windowEnd,Integer.parseInt(hget)));

        }
    }

    // 自定义布隆过滤器
    private static class UserVisitBloomFilter{
        //定义布隆过滤器容量,最好传入2的整次幂数据
        private Long cap;

        public UserVisitBloomFilter(Long cap){
            this.cap = cap;
        }

        //传入一个字符串,获取在BitMap中的位置
        // Hash算法，因为31是一个质数
        public Long getOffSet(String value){
            Long result = 0L;
            for (char ch :value.toCharArray()){
                result = result * 31 + ch;
            }
            // 取模 位运算
            return  result & (cap -1);
        }

    }

    // 自定义触发器 继承 Trigger 类

    /***
     * TriggerResult 类 四个枚举类型
     *  FIRE_AND_PURGE 输出并计算
     *  FIRE  只计算，不输出
     *  PURGE 只输出，不计算
     *  CONTINUE 什么也不做
     */
    private static class UserVisitTrigger extends Trigger<UserBehavior,TimeWindow>{
        // 在窗口中每进入一条数据的时候调用一次
        @Override
        public TriggerResult onElement(UserBehavior element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        // 在一个ProcessingTime定时器触发的时候调用
        // 在此案例中，以 processTime和EventTime 都没关系，所以都不做任何处理
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        // 会在一个EventTime定时器触发的时候调用
        // 在此案例中，以 processTime和EventTime 都没关系，所以都不做任何处理
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        // 方法会在窗口清除的时候调用
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

        }
    }
}

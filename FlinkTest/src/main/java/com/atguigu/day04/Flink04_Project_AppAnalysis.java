package com.atguigu.day04;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

// 按照渠道与行为计算
public class Flink04_Project_AppAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MarketingUserBehavior> marketingUserBehaviorDSt =
                env.addSource(new AppMarketingDataSource());


        KeyedStream<MarketingUserBehavior, String> keyedStream = marketingUserBehaviorDSt.keyBy(
                new KeySelector<MarketingUserBehavior, String>() {
            @Override
            public String getKey(MarketingUserBehavior value) throws Exception {
                return value.getBehavior();
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedStream.process(
                new KeyedProcessFunction<String, MarketingUserBehavior, Tuple2<String, Long>>() {
            HashMap<String, Long> hashMap = new HashMap();

            @Override
            public void processElement(MarketingUserBehavior value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                Long count = hashMap.getOrDefault(value.getBehavior(), 0L);
                count++;
                hashMap.put(value.getBehavior(), count);
                out.collect(new Tuple2<>(ctx.getCurrentKey(), count));
            }
        });

        result.print();


        env.execute(Flink04_Project_AppAnalysis.class.getName());
    }

    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior>{
        private Random random = new Random();
        private List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        private List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");
        private Boolean canRun = true;

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(10000),
                        channels.get(random.nextInt(channels.size())),
                        behaviors.get(random.nextInt(behaviors.size())),
                        System.currentTimeMillis());

                ctx.collect(marketingUserBehavior);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }
}

package com.atguigu.day05;

import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.sql.Timestamp;

public class Flink01_Window_TimeTumbling {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] split = value.split(" ");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        // 按照key进行分区
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToOne.keyBy(data -> data.f0);

        // 开窗
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // 增量聚合计算
//         windowedStream.sum(1).print();
        // 即进行增量聚合，又可以获得里面的窗口信息，注意这时增量的输出，就是全量的输入,同时在全量的迭代器中只有一条数据
//        SingleOutputStreamOperator<Tuple2<String, Integer>> result =
//                windowedStream.aggregate(new MyAggregate(),new MyWindowFunc());

        // 全量计算
        // a. apply
       /*SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.apply(
                new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Tuple2<String, Integer>> out) throws Exception {
                        window.getStart(); // 获取窗口的开始时间
                        window.getEnd();  // 获取窗口额结束时间

                        // 计算wordCount，input内迭代器的长度，就是wordCount
                        // 将iterator 转换为 ArrayList
                        ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(input.iterator());
                        // out.collect(new Tuple2<>(key, arrayList.size()));
                        out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key,arrayList.size()));
                    }
                });*/

       // b. process
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.process(new ProcessWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements,
                                Collector<Tuple2<String, Integer>> out) throws Exception {

                ArrayList<Tuple2<String, Integer>> arrayList = Lists.newArrayList(elements.iterator());
                out.collect(new Tuple2<>(new Timestamp(context.window().getStart()) + ":" + key, arrayList.size()));

            }
        });


        result.print();
        env.execute(Flink01_Window_TimeTumbling.class.getName());
    }

    private static class MyAggregate implements AggregateFunction<Tuple2<String, Integer>, Integer, Integer> {
        // 累加器
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        // 累加计算
        @Override
        public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
            return accumulator +1;
        }

        // 获取计算结果并返回
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        // merge,分区间聚合
        @Override
        public Integer merge(Integer a, Integer b) {
            return a+b;
        }
    }

    private static class MyWindowFunc implements WindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow>{
        @Override
        public void apply(String key, TimeWindow window, Iterable<Integer> input,
                          Collector<Tuple2<String, Integer>> out) throws Exception {

            // 从迭代器中获取数据
            Integer next = input.iterator().next();
            // 返回值
            out.collect(new Tuple2<>(new Timestamp(window.getStart()) + ":" + key,next));

        }
    }
}

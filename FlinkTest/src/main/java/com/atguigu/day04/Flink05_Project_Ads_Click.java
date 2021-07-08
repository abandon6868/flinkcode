package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/*
*
* 各省份页面广告点击量实时统计
* */
public class Flink05_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/AdClickLog.csv");

        SingleOutputStreamOperator<AdsClickLog> adsClickLogSO = dataStreamSource.flatMap(new FlatMapFunction<String, AdsClickLog>() {
            @Override
            public void flatMap(String value, Collector<AdsClickLog> out) throws Exception {
                String[] infos = value.split(",");
                out.collect(new AdsClickLog(Long.parseLong(infos[0]),
                        Long.parseLong(infos[1]),
                        infos[2],
                        infos[3],
                        Long.parseLong(infos[4])));
            }
        });

        KeyedStream<AdsClickLog, Tuple2<String, Long>> adsKeyedStream= adsClickLogSO.keyBy(new KeySelector<AdsClickLog, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> getKey(AdsClickLog value) throws Exception {
                return new Tuple2<>(value.getProvince(), value.getAdId());
            }
        });

        SingleOutputStreamOperator<Tuple2<Tuple2<String, Long>, Long>> result = adsKeyedStream.process(new KeyedProcessFunction<Tuple2<String, Long>,
                AdsClickLog, Tuple2<Tuple2<String, Long>, Long>>() {
            HashMap<String, Long> hashMap = new HashMap<>();

            @Override
            public void processElement(AdsClickLog value,
                                       Context ctx, Collector<Tuple2<Tuple2<String, Long>, Long>> out)
                    throws Exception {
                String key = value.getProvince() + "-" + value.getAdId();
                Long count = hashMap.getOrDefault(key, 0L);
                count++;
                hashMap.put(key, count);
                out.collect(new Tuple2<>(ctx.getCurrentKey(), count));
            }
        });

        result.print();

        env.execute(Flink05_Project_Ads_Click.class.getName());
    }
}

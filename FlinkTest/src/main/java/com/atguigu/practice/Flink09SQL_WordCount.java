package com.atguigu.practice;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

public class Flink09SQL_WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOnes =
                dataStreamSource.flatMap(
                        new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                    throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(new Tuple2<>(s, 1));
                }
            }
        });

        // 注册表
        tableEnv.createTemporaryView("wordCount",wordToOnes,$("word"),$("cnt"));

        // 查询数据，并按照word进行分组
        Table select = tableEnv.sqlQuery("select word,count(word) from wordCount group by word");
//        Table select = tableEnv.sqlQuery("select word,cnt from wordCount");
//        tableEnv.toRetractStream(select, Row.class).print();
        // 使用kafka作为source 消费数据
        tableEnv.connect(new Kafka()
                    .version("universal")
                    .topic("test")
                    .startFromLatest()
                    .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withSchema(new Schema()
                    .field("word", DataTypes.STRING())
                    .field("count",DataTypes.BIGINT()))
                .withFormat(new Json())
                .createTemporaryTable("WordCount");
        // 使用ES连接器
        tableEnv.connect(new Elasticsearch()
                    .version("6")
                    .index("wordCount")
                    .documentType("_doc")
                    .bulkFlushMaxActions(1)
                    .host("hadoop102",9200,"http"))
                .withSchema(new Schema()
                        .field("word",DataTypes.STRING())
                        .field("count",DataTypes.BIGINT())
                ).withFormat(new Json())
                .createTemporaryTable("wordCounts");

        tableEnv.executeSql("insert into wordCounts select * from WordCount");
    }
}

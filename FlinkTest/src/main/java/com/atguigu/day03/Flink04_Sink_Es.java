package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;

public class Flink04_Sink_Es {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
//                env.socketTextStream("hadoop102", 9999);

        SingleOutputStreamOperator<WaterSensor> waterSensorDS =
                streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
            }
        });

        waterSensorDS.print();
        // 将结果写入到ES中
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder =
                new ElasticsearchSink.Builder<WaterSensor>(httpHosts,new MyEsSinkFunc());
        ElasticsearchSink<WaterSensor> elasticsearchSink = waterSensorBuilder.build();

        waterSensorDS.addSink(elasticsearchSink);

        env.execute(Flink04_Sink_Es.class.getName());
    }

    public static class MyEsSinkFunc implements ElasticsearchSinkFunction<WaterSensor> {
        @Override
        public void process(WaterSensor element, RuntimeContext ctx, RequestIndexer indexer) {
            HashMap<String, String> map = new HashMap<>();
            map.put("ts",element.getTs().toString());
            map.put("vc",element.getVc().toString());

            IndexRequest source = Requests.indexRequest()
                    .index("sensor")
                    .type("_doc")
                    .id(element.getId())
                    .source(map);

            indexer.add(source);
        }
    }
}

package com.atguigu.day02;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class Flink06_Source_MySource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> streamSource = env.addSource(
                new MySource("hadoop102", 9999));

        streamSource.print();

        env.execute(Flink06_Source_MySource.class.getName());


    }

    public static class MySource implements SourceFunction<WaterSensor>{

        Boolean running = true;
        // 自定义主机和端口
        private String host;
        private Integer port;

        Socket socket = null;
        BufferedReader reader = null;

        public String getHost() {
            return host;
        }

        public void setHost(String host) {
            this.host = host;
        }

        public Integer getPort() {
            return port;
        }

        public void setPort(Integer port) {
            this.port = port;
        }

        public MySource(String host, Integer port) {
            this.host = host;
            this.port = port;
        }

        public MySource() {
        }

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            socket = new Socket(host,port);
            reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // 读取数据
            String line = reader.readLine();

            while (running && line != null ) {
                String[] split = line.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0],
                        Long.parseLong(split[1]), Integer.parseInt(split[2]));

                ctx.collect(waterSensor);
                line = reader.readLine();
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                socket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

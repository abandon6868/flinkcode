package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class Flink05_Sink_Mysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0],
                        Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        waterSensorDS.print();
        // 将数据写入mysql
        waterSensorDS.addSink(new MySinkMysql());
        env.execute(Flink05_Sink_Mysql.class.getName());
    }

    public static class MySinkMysql extends RichSinkFunction<WaterSensor> {
        // 声明连接
        private Connection connection;
        private PreparedStatement preparedStatement;
        private String userName = "root";
        private String password = "admin";
        private String sql ="INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?";

        // 生命周期方法，用于创建mysql连接
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection(
                    "jdbc:mysql://hadoop102:3306/test?useSSL=false",
                    userName,
                    password);

            preparedStatement = connection.prepareStatement(
                    "INSERT INTO `sensor` VALUES(?,?,?) ON DUPLICATE KEY UPDATE `ts`=?,`vc`=?");

        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            //给占位符赋值
            preparedStatement.setString(1,value.getId());
            preparedStatement.setLong(2,value.getTs());
            preparedStatement.setInt(3,value.getVc());
            preparedStatement.setLong(4,value.getTs());
            preparedStatement.setInt(5,value.getVc());
            //执行操作
            preparedStatement.execute();
        }

        // 生命周期方法，用于关闭mysql连接
        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}

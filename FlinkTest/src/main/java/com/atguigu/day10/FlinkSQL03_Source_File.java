package com.atguigu.day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.$;

/***
 * 从文件系统读取数据，读取外部文本数据
 */
public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        // 创建env环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 使用connect的方式 读取外部文件数据
        tableEnv.connect(new FileSystem()  // 外部文件系统
                .path("F:\\myFlink\\FlinkTest\\input\\sensor.txt")) // path路径
                .withSchema(new Schema()    // 设计schema
                    .field("id", DataTypes.STRING())
                    .field("ts",DataTypes.BIGINT())
                    .field("vc",DataTypes.INT()))
                .withFormat(new Csv().fieldDelimiter(',')  // 输入数据的格式
                        .lineDelimiter("\n"))
                .createTemporaryTable("sensor");  // 创建表

        // 将连接器作用于表
        Table sensor = tableEnv.from("sensor");

        // 查询
        Table select = sensor.groupBy($("id"))
                .select($("id"), $("id").count());

        // 将数据转换为流进行输出
        DataStream<Tuple2<Boolean, Row>> tuple2DataStream =
                tableEnv.toRetractStream(select, Row.class);

        tuple2DataStream.print();

        env.execute();
    }
}

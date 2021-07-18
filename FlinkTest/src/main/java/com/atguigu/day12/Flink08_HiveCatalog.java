package com.atguigu.day12;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class Flink08_HiveCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2 创建 hiveCatLog
        HiveCatalog hiveCatalog =
                new HiveCatalog("myHive",
                        "default",
                        "input");

        //3 注册HiveCatLog
        tableEnv.registerCatalog("myHive",hiveCatalog);

        //4 使用HiveCatLog
        tableEnv.useCatalog("myHive");

        //5 执行查询，查询Hive中已经存在的表的数据,也可以创建表
        // tableEnv.executeSql("create table ssx(id int,name string)");

        tableEnv.executeSql("select * from hive_map").print();

    }
}

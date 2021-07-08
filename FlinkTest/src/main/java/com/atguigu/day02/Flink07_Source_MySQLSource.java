package com.atguigu.day02;

import com.atguigu.bean.Student;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.*;

public class Flink07_Source_MySQLSource {
    public static void main(String[] args) throws Exception {
        // 1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2 添加数据源，使用Mysql 作为数据源
        DataStreamSource<Student> streamSource = env.addSource(new MyJdbc());
        // 3 打印数据
        streamSource.print();
        // 4 开始执行
        env.execute(Flink07_Source_MySQLSource.class.getName());
    }

    public static class MyJdbc extends RichSourceFunction<Student> {
        private PreparedStatement ps = null;
        private Connection conn = null;
        private String url = "jdbc:mysql://hadoop102:3306/test?useUnicode=true&characterEncoding=UTF-8";
        private String userName = "root";
        private String passWord = "admin";
        private Boolean running = true;
        private ResultSet resultSet;

        @Override
        public void run(SourceContext<Student> ctx) throws Exception {
//            conn = getConn();
//            String sql = "select id,name,age from stu1";
//            ps = this.conn.prepareStatement(sql);
            resultSet = ps.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                int age = resultSet.getInt("age");
                Student student = new Student(id, name, age);
                ctx.collect(student);
            }
        }

        @Override
        public void cancel() {
            running = false;
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                ps.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        // 建立mysql 连接器
        private Connection getConn(){
            try {
              Class.forName("com.mysql.jdbc.Driver");
              conn = DriverManager.getConnection(url, userName, passWord);
            } catch (ClassNotFoundException | SQLException e) {
                e.printStackTrace();
                System.out.println("Mysql Connect has exception,msg="+e.getMessage());
            }
            return conn;
        }

        // 获取数据
        @Override
        public void open(Configuration parameters) throws SQLException {
            conn = getConn();
            String sql = "select * from stu1;";
            ps = conn.prepareStatement(sql);
        }

    }
}


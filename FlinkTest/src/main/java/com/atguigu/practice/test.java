package com.atguigu.practice;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[] args) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date parse = dateFormat.parse("2017-11-26 17:00:00.0");
        long time = parse.getTime();
        System.out.println(time);
        Timestamp timestamp = new Timestamp(time);

        System.out.println(timestamp);
    }
}

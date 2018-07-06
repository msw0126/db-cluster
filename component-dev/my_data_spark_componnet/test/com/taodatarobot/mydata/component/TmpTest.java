package com.taodatarobot.mydata.component;

import com.taodatarobot.mydata.component.util.Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

import java.util.Properties;

public class TmpTest {
    public static void main(String[] args) {
        SQLContext sc = Util.getSQLContext();
        String selectSql = "auth_user";

        // 指定用户名、密码
        Properties properties = new Properties();
        properties.setProperty("user", "root");
        properties.setProperty("password", "123456");

        // 读取数据
        Dataset data = sc.read().jdbc("jdbc:mysql://127.0.0.1:3306/databrain", selectSql, properties);
        data.printSchema();
    }
}

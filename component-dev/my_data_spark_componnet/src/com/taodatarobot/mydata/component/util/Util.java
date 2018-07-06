package com.taodatarobot.mydata.component.util;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

public class Util {
    public static SQLContext getSQLContext(){
        SparkSession sparkSession = getSparkSession();
        SparkContext sparkContext = sparkSession.sparkContext();
        return new SQLContext(sparkContext);
    }

    public static SparkSession getSparkSession(){
        return SparkSession.builder()
                .master("local")
                .getOrCreate();
    }
}

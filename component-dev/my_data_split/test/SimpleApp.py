# -*- coding:utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("Python") \
        .config("spark.some.config.option", "some-value") \
        .master("local") \
        .getOrCreate()

    data = spark.read.parquet("hdfs://node1:8020/system/new_file15.parquet")
    # data = spark.read.parquet("hdfs://node1:8020/system/audits.parquet")
    # data = spark.read.parquet("hdfs://node1:8020/system/percentSplit/percentSplit/table_a/par*")
    # data = spark.read.parquet("hdfs://node1:8020/system/percentSplit/percentLayered/table_a/par*")
    # data = spark.read.parquet("hdfs://node1:8020/system/percentSplit/rule_split/table_a/par*")

    data.createOrReplaceTempView("table")
    table_a = spark.sql(str.format("select * from TABLE"))
    table_a.show(1000)
    # table_b.show(10)

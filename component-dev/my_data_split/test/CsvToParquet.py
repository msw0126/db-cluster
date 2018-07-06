from pyspark import SparkContext
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import *


if __name__ == "__main__":
    # sc = SparkContext(appName="CSV2Parquet")
    # sqlContext = SQLContext(sc)
    spark = SparkSession \
        .builder \
        .appName("Python") \
        .config("spark.some.config.option", "some-value") \
        .master("local") \
        .getOrCreate()


    # rdd = spark.textFile("I:\\AUDITS.csv").map(parse)
    rdd = spark.read.csv("hdfs://node1:8020/system/file15_trim.csv")
    rdd.write.parquet("hdfs://node1:8020/system/file15_trim")
    # df = spark.createDataFrame(rdd, schema)
    # df.write.parquet('I:\\AUDITS.par')

    # rdd = spark.read.parquet("hdfs://node1:8020/system/new_file15.parquet")
    # rdd.write.csv("hdfs://node1:8020/system/new_file15.csv")
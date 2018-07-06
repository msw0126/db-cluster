# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

from pyspark.sql.functions import monotonically_increasing_id


class PercentSplit(object):
    """
    按比例拆分
    """
    def __init__(self, spark, input_path, percent, seed, field, output_path_a, output_path_b):
        self.spark = spark
        self.input_path = input_path
        self.percent = percent
        self.seed = seed
        self.field = field
        self.output_path_a = output_path_a
        self.output_path_b = output_path_b


    def percent_split(self):
        """
        按比例拆分
        :return:
        """
        data = self.spark.read.parquet(self.input_path)
        data.persist()
        counts = data.count()
        before_row = int(counts * float(self.percent))
        # todo 保证tmp_id_column不出现在数据中
        tmp_id_column = "taoshu_tmp_id"
        data_tmp = data.withColumn(tmp_id_column, monotonically_increasing_id())
        data_tmp.createOrReplaceTempView("table")
        SQL_A = """SELECT * 
                FROM TABLE
                where %s < %d
                """ \
              % (tmp_id_column, before_row)
        SQL_B = """SELECT * 
                FROM TABLE
                where %s >= %d""" \
                % (tmp_id_column, before_row)
        table_a = self.spark.sql(SQL_A).drop(tmp_id_column)
        table_b = self.spark.sql(SQL_B).drop(tmp_id_column)
        table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
        table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)
        table_a.show(1000)
        table_b.show(1000)


    def percent_random(self):
        """
        按比例加随机种子拆分
        :return:
        """
        other_percent = 1 - self.percent
        seed = int(self.seed)
        data = self.spark.read.parquet(self.input_path)
        data.persist()
        splits = data.randomSplit((self.percent, other_percent), seed)
        table_a = splits[0]
        table_b = splits[1]
        table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
        table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)
        table_a.show(1000)
        table_b.show(1000)


    def percent_layered(self):
        """
        按比例分层拆分
        :return:
        """
        data = self.spark.read.parquet(self.input_path)
        data.persist()
        percent = float(self.percent)
        tmp_id_column = "taoshu_tmp_id"
        field = str(self.field)
        row_num = "taoshu_row_num"
        row_count = "taoshu_row_count"
        data = data.withColumn(tmp_id_column, monotonically_increasing_id())
        data.registerTempTable("TABLE")
        SQL_A = """SELECT *  
                   FROM (
                        SELECT *, ROW_NUMBER() OVER(partition BY %s ORDER BY %s DESC ) AS %s, COUNT (*) OVER(partition BY %s) AS 
                   %s 
                        FROM TABLE 
                   ) ranked 
                   WHERE ranked.%s >= 1  
                        AND ranked.%s <= (ranked.%s * %f)""" \
                % (field, tmp_id_column, row_num, field, row_count, row_num, row_num, row_count, percent)

        SQL_B = """SELECT *  
                   FROM (
                        SELECT *, ROW_NUMBER() OVER(partition BY %s ORDER BY %s DESC ) as %s, COUNT (*) OVER(partition BY %s) AS 
                   %s 
                        FROM TABLE 
                   ) ranked 
                   WHERE ranked.%s >= 1  
                        AND ranked.%s > (ranked.%s * %f)""" \
                % (field, tmp_id_column, row_num, field, row_count, row_num, row_num, row_count, percent)

        table_a = self.spark.sql(SQL_A).drop(tmp_id_column).drop(row_num).drop(row_count)
        table_b = self.spark.sql(SQL_B).drop(tmp_id_column).drop(row_num).drop(row_count)
        table_a.show(1000)
        table_b.show(1000)
        table_a.repartition(1).write.mode( "overwrite" ).parquet(self.output_path_a)
        table_b.repartition(1).write.mode( "overwrite" ).parquet(self.output_path_b)

    def percent_layered_random(self):
        """
        按比例分层加随机种子拆分
        :return:
        """
        data = self.spark.read.parquet(self.input_path)
        data.persist()
        percent = float(self.percent)
        seed = int(self.seed)
        tmp_id_column = "taoshu_tmp_id"
        field = str(self.field)
        row_num = "taoshu_row_num"
        row_count = "taoshu_row_count"
        data = data.withColumn(tmp_id_column, monotonically_increasing_id())
        data.registerTempTable("table")

        SQL_A = """SELECT *  
                   FROM (
                        SELECT *, ROW_NUMBER() OVER(partition BY %s ORDER BY %s DESC ) AS %s, COUNT (*) OVER(partition BY %s) AS 
                   %s 
                        FROM TABLE 
                   ) ranked 
                   WHERE ranked.%s >= 1  
                        AND ranked.%s <= (ranked.%s * %f) 
                   ORDER BY rand(%s)""" \
                % (field, tmp_id_column, row_num, field, row_count, row_num, row_num, row_count, percent, seed)

        SQL_B = """SELECT *  
                   FROM (
                        SELECT *, ROW_NUMBER() OVER(partition BY %s ORDER BY %s DESC ) AS %s, COUNT (*) OVER(partition BY %s) AS 
                   %s 
                        FROM TABLE 
                   ) ranked 
                   WHERE ranked.%s >= 1  
                        AND ranked.%s > (ranked.%s * %f) 
                   ORDER BY rand(%s)""" \
                % (field, tmp_id_column, row_num, field, row_count, row_num, row_num, row_count, percent, seed)

        table_a = self.spark.sql(SQL_A).drop(tmp_id_column).drop(row_num).drop(row_count)
        table_b = self.spark.sql(SQL_B).drop(tmp_id_column).drop(row_num).drop(row_count)
        table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
        table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)
        table_a.show(1000)
        table_b.show(1000)

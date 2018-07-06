# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""
from pyspark.sql.functions import monotonically_increasing_id


class SortSplit(object):
    """
    先排序后拆分
    """
    def __init__(self, spark, input_path, percent, field, order, output_path_a, output_path_b):
        self.spark = spark
        self.input_path = input_path
        self.percent = percent
        self.field = field
        self.order = order
        self.output_path_a = output_path_a
        self.output_path_b = output_path_b

    def sort_split(self):
        """
        先排序后拆分
        :return:
        """
        data = self.spark.read.parquet(self.input_path)
        counts = data.count()
        before_row = int(counts * self.percent)
        tmp_id_column = "taoshu_tmp_id"
        if self.order == 1:
            """
            正序排序拆分
            """
            data = data.withColumn(tmp_id_column, monotonically_increasing_id())
            data.createOrReplaceTempView("table")

            SQL_A = """SELECT * 
                       FROM (
                            SELECT * 
                            FROM TABLE 
                            ORDER BY %s ASC 
                       ) 
                       WHERE %s < %d""" \
                    % (self.field, tmp_id_column, before_row)

            SQL_B = """SELECT * 
                       FROM (
                            SELECT * 
                            FROM TABLE 
                            ORDER BY %s ASC 
                       ) 
                       WHERE %s < %d""" \
                    % (self.field, tmp_id_column, before_row)

            table_a = self.spark.sql(SQL_A).drop(tmp_id_column)
            table_b = self.spark.sql(SQL_B).drop(tmp_id_column)
            table_a.show(1000)
            table_b.show(1000)
            table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
            table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)
        elif self.order == 0:
            """
            倒序排序拆分
            """
            row = int(counts - before_row)
            data = data.withColumn(tmp_id_column, monotonically_increasing_id())
            data.createOrReplaceTempView("table")

            SQL_A = """SELECT * 
                       FROM (
                            SELECT * 
                            FROM TABLE 
                            ORDER BY %s DESC 
                       )
                       WHERE %s >= %d""" \
                    % (self.field, tmp_id_column, row)

            SQL_B = """SELECT * 
                       FROM (
                            SELECT * 
                            FROM TABLE 
                            ORDER BY %s DESC 
                       )
                       WHERE %s < %d""" \
                    % (self.field, tmp_id_column, row)

            table_a = self.spark.sql(SQL_A).drop(tmp_id_column)
            table_b = self.spark.sql(SQL_B).drop(tmp_id_column)
            table_a.show(1000)
            table_b.show(1000)
            table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
            table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)
        else:
            print "Please write parameters 1 or 0."

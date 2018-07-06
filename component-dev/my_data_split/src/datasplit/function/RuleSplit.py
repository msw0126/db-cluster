# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

class RuleSplit(object):
    """
    按规则拆分
    """
    def __init__(self, spark, input_path, rule_sql, output_path_a, output_path_b):
        self.spark = spark
        self.input_path = input_path
        self.rule_sql = rule_sql
        self.output_path_a = output_path_a
        self.output_path_b = output_path_b

    def rule_split(self):
        """
        按规则拆分
        :return:
        """
        data = self.spark.read.parquet(self.input_path)
        rule_sql = str(self.rule_sql)
        data.createOrReplaceTempView("table")
        SQL_A = """SELECT *
                    FROM TABLE
                    WHERE %s""" \
                % rule_sql

        SQL_B = """SELECT *
                    FROM TABLE
                    EXCEPT
                    (SELECT *
                    FROM TABLE
                    WHERE %s)""" \
                % rule_sql

        table_a = self.spark.sql(SQL_A)
        table_b = self.spark.sql(SQL_B)
        table_a.show(1000)
        table_b.show(1000)
        table_a.repartition(1).write.mode("overwrite").parquet(self.output_path_a)
        table_b.repartition(1).write.mode("overwrite").parquet(self.output_path_b)

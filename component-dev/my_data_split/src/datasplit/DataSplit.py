# -*- coding:utf-8 -*-
"""
Module Description: DataBrain 数据拆分
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

import datetime
from pyspark.sql import SparkSession

from datasplit.bean.ConfigParser import MyConfigParser
from datasplit.function import Util
from datasplit.function.PercentSplit import PercentSplit
from datasplit.function.RuleSplit import RuleSplit
from datasplit.function.SortSplit import SortSplit


class DataSplit(object):
    def __init__(self, config_path):
        self.config_path = config_path

    def run(self):
        t0 = datetime.datetime.now()
        config_path = Util.unix_abs_path(self.config_path)

        spark = SparkSession \
            .builder \
            .getOrCreate()
        # spark = SparkSession \
        #     .builder \
        #     .appName("data_split_test04") \
        #     .master("local") \
        #     .getOrCreate()

        #解析配置文件
        cf = MyConfigParser(config_path)
        config_info = cf.parse()
        split_method = config_info.keys()[0]
        split_method_config_info = cf.config_parser_main(split_method)

        #获取拆分方法参数
        input_path = split_method_config_info.get("input_path")
        percent = split_method_config_info.get("percent")
        seed = split_method_config_info.get("seed")
        rule_sql = split_method_config_info.get("rule_sql")
        order = split_method_config_info.get("order")
        field = split_method_config_info.get("field")
        output_path_a = split_method_config_info.get("output_path_a")
        output_path_b = split_method_config_info.get("output_path_b")

        # 将类实例化
        ps = PercentSplit(spark, input_path, percent, seed, field, output_path_a, output_path_b)
        rs = RuleSplit(spark, input_path, rule_sql, output_path_a, output_path_b)
        ss = SortSplit(spark, input_path, percent, field, order, output_path_a, output_path_b)

        #判断拆分方法
        if u"percent_split" in config_info.keys():
            ps.percent_split()
        elif u"percent_random" in config_info.keys():
            ps.percent_random()
        elif u"percent_layered" in config_info.keys():
            ps.percent_layered()
        elif u"percent_layered_random" in config_info.keys():
            ps.percent_layered_random()
        elif u"rule_split" in config_info.keys():
            rs.rule_split()
        elif u"sort_split" in config_info.keys():
            ss.sort_split()
        else:
            print "split_method doesn't exist. Please check."

        spark.stop()
        print "total time cost:" + str(datetime.datetime.now() - t0)
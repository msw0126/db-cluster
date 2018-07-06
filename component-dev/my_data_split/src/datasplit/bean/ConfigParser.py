# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

import json
import os


class MyConfigParser(object):
    """
    配置文件解析
    """
    def __init__(self, config_path):
        self.config_path = config_path

    def parse(self):
        """
        读取配置文件
        :return:
        """
        if not os.path.exists(self.config_path):
            raise Exception("Configuration file", self.config_path,
                             "doesn't exist. Please check the config path." )

        with open(self.config_path, 'r') as f:
            config_json = json.load(f)
        return config_json


    def config_parser_main(self, split_method):
        """
        获取拆分方法参数dict
        :param split_method:
        :return:
        """
        config_info = self.parse()
        if split_method in config_info.keys():
            split_method = config_info.keys()[0]
            split_method_config_info = config_info.get(split_method)
        return split_method_config_info

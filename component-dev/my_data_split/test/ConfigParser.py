# -*- coding:utf-8 -*-

import json
import os


class MyConfigParser(object):
    def __init__(self, config_path):
        self.config_path = config_path


    def parse(self):
        if not os.path.exists(self.config_path):
            raise Exception("Configuration file", self.config_path,
                             "doesn't exist. Please check the config path." )

        with open(self.config_path, 'r') as f:
            config_json = json.load(f)
        return config_json


    def config_parser_main(self, split_method):
        config_info = self.parse()
        if split_method in config_info.keys():
            split_method = config_info.keys()[0]
            split_method_config_info = config_info.get(split_method)
        return split_method_config_info

if __name__ == '__main__':
    config_path = "F:\\work\\databrain-cluster\\component-dev\\my_data_split\\tdir\\data_split_config.json"

    mcf = MyConfigParser(config_path)
    split_method = mcf.parse().keys()[0]
    print split_method

    split_method_config_info = mcf.config_parser_main(split_method)
    print split_method_config_info


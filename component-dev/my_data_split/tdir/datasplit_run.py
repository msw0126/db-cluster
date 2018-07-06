# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

from datasplit.DataSplit import *
import sys
reload(sys)
sys.setdefaultencoding('utf8')

if __name__ == '__main__':
    """
    提交本地或集群任务入口
    """
    config_path = sys.argv[1]
    data_split = DataSplit(config_path)
    data_split.run()


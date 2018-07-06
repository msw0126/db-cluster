# -*- coding:utf-8 -*-
"""
Module Description:
Date: 2018/1/10
Author: ShuaiWei.Meng
"""

import re
import os



def unix_abs_path(path, pre_path=''):
    """
    校验配置文件路径
    :param path:
    :param pre_path:
    :return:
    """
    if not os.path.isabs(path):
        path = os.path.join(pre_path, path)
        path = os.path.abspath(path)

    path_sep = re.split("[\\\\]", path)
    path_rec = list()
    for pp in path_sep:
        pp = pp.strip()
        if pp == '':
            continue
        path_rec.append(pp)
    return "/".join(path_rec)

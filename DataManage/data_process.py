#!/usr/bin/python
# -*- encoding: utf-8 -*-
"""
@Author: apophis
@File: data_process.py
@Time: 2024/4/5 15:26
@Description: 工程描述
"""
import os
import numpy as np
import pandas as pd
from csv_to_mysql import run

np.set_printoptions(suppress=True)
pd.set_option('display.float_format', lambda x: '%.3f' % x)  # 小数点后面保留3位小数，诸如此类，按需修改吧
output_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__))).replace("\\", "/") + "/output/"
origin_path = output_path.replace("//", "/") + "origin/"


def column_to_int():
    """
    将 user_id 和 item 类型转换成数值型
    默认对所有数据所有数据进行转换
    :return:
    """
    df = pd.read_csv(origin_path + "douban_users.csv")

    user_codes = df.user_id.drop_duplicates().reset_index()
    user_codes.rename(columns={'index': 'u_index'}, inplace=True)
    user_codes['us_index_value'] = list(user_codes.index)
    small_set = pd.merge(df, user_codes, how='left')
    small_set["it_index_value"] = small_set["movie_id"]
    small_set["fractional_play_count"] = small_set["rating"]
    mat_candidate = small_set[['us_index_value', 'it_index_value', 'fractional_play_count', 'user_id']]

    mat_candidate.to_csv(origin_path + "ratings.csv", index=False)
    # 记录映射关系
    user_codes.to_csv(origin_path + "user_codes.csv", index=False, columns=["user_id", "us_index_value"])

    item_codes = pd.read_csv(origin_path + "douban_movies.csv")
    item_codes["it_index_value"] = item_codes["movie_id"]
    item_codes["item"] = item_codes["name"]
    item_codes.to_csv(origin_path + "item_codes.csv", index=False, columns=["item", "it_index_value"])
    # 录入mysql
    run(['user_codes', 'item_codes'])


if __name__ == '__main__':
    column_to_int()

#!/usr/bin/python
# -*- encoding: utf-8 -*-
"""
@Author: apophis
@File: csv_to_mysql.py
@Time: 2024/3/22 17:33
@Description: Project description
"""
import os
import pandas as pd
import pymysql
import traceback

output_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__))).replace("\\", "/") + "/output/"
origin_path = output_path.replace("//", "/") + "origin/"

conn = pymysql.connect(host='localhost',  # Database address
                       user='root',  # Username
                       password='123456',  # Password
                       db='recommendation',  # Database name
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor)  # Character Encoding


def write_mysql(name):
    cursor = conn.cursor()
    query = f"""
            SELECT COLUMN_NAME 
            FROM information_schema.COLUMNS 
            WHERE TABLE_SCHEMA = 'recommendation' AND TABLE_NAME = '{name}';
        """
    cursor.execute(query)
    # Get all field names
    columns = [row['COLUMN_NAME'] for row in cursor.fetchall()]
    cursor.close()

    with conn.cursor() as cursor1:
        join = ','.join(columns)
        s = ','.join(['%s' for _ in range(len(columns))])
        sql_template = f"INSERT INTO {name} ({join}) VALUES ({s})"
        csv = pd.read_csv(f"{origin_path}{name}.csv", delimiter=",", encoding="utf8")
        arr = []
        for index, row in csv.iterrows():
            tmp = []
            for column in columns:
                tmp.append(row[column])
            arr.append(tmp)
        try:
            cursor1.executemany(sql_template, arr)
            conn.commit()
        except Exception:
            conn.rollback()
            traceback.print_exc()
        finally:
            cursor1.close()


def run(t_names):
    try:
        for t_name in t_names:
            write_mysql(t_name)
    finally:
        conn.close()


if __name__ == '__main__':
    # Enter mysql
    run(['user_codes', 'item_codes'])

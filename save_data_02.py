# -*- coding:utf-8 -*-
import scipy.io as sio
import pickle
import pandas as pd
import numpy as np
import os
import pymongo as mg
import json
from load_data import load_day_data_from_wind
from global_variables import *


def status_process(status):
    if status == "DR":
        return 1
    elif status == "N":
        return 2
    elif status == "XD":
        return 3
    elif status == "XR":
        return 4
    elif status == "停牌":
        return 5
    elif status == "交易":
        return 0

def stock_name_process(stock_name):
    return stock_name.split(".")[0]

def insert_wd_day_market_data():
    df_columes = ["Stock", "Date", "preClose", "Open", "High", "Low", "Close", "Change", "PCTChange",
                           "Volume", "Value", "ADJ", "Mean", "STATUS",]
    index_col = [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 17, 18, 19]
    for year in range(2010, 2018):
        print(year)
        dt = pd.read_csv("stock_market_data_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
        dt["STATUS"] = dt["STATUS"].map(status_process)
        dt["Stock"] = dt["Stock"].map(stock_name_process)
        database_day["WIND_ALL"].insert_many(json.loads((dt.to_json(orient="records"))))
    print("OK")

def insert_derive_data():
    df_columes = np.arange(35)
    index_col = list(range(36))
    index_col.remove(2)
    for year in range(2010, 2018):
        print(year)
        if year != 2016:
            dt = pd.read_csv("stock_market_derive_data_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
        else:
            for j in ["01","02"]:
                dt = pd.read_csv("stock_market_derive_data_%d_%s.csv" % (year, j), encoding="gbk", usecols=index_col,
                                 names=df_columes)
                dt = dt.sort_values(by = 1)
        database_day["DERIVE"].insert_many(json.loads((dt.to_json(orient="records"))))
    print("OK!")

def insert_industry_members_info():
    index_col = [0, 1, 2, 3]
    df_columes = ["Industry", "Stock", "Date_in", "Date_out"]
    dt = pd.read_csv("industry_members.csv", encoding="gbk", usecols=index_col, names=df_columes)
    print("OK!")



if __name__ == "__main__":
    import os
    os.chdir("G:\\stock_data")
    # insert_wd_day_market_data()
    # insert_industry_members_info()
    insert_derive_data()
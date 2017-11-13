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
    for year in range(2016, 2018):
        print(year)
        if year != 2016:
            dt = pd.read_csv("stock_market_derive_data_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
            dt[0] = dt[0].map(stock_name_process)
            database_day["DERIVE"].insert_many(json.loads((dt.to_json(orient="records"))))
        else:
            for j in ["01","02"]:
                dt = pd.read_csv("stock_market_derive_data_%d_%s.csv" % (year, j), encoding="gbk", usecols=index_col,names=df_columes)
                dt[0] = dt[0].map(stock_name_process)
                database_day["DERIVE"].insert_many(json.loads((dt.to_json(orient="records"))))

    print("OK!")

def insert_moneyflow_data():
    df_columes = np.arange(97)
    index_col = list(range(97))
    for year in range(2010, 2018):
        print(year)
        if year in [2010, 2011, 2012]:
            dt = pd.read_csv("ashare_moneyflow_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
            dt[0] = dt[0].map(stock_name_process)
            database_day["DERIVE"].insert_many(json.loads((dt.to_json(orient="records"))))
        else:
            for j in ["01","02"]:
                dt = pd.read_csv("ashare_moneyflow_%d_%s.csv" % (year, j), encoding="gbk", usecols=index_col, names=df_columes)
                dt[0] = dt[0].map(stock_name_process)
                database_day["MONEYFLOW"].insert_many(json.loads((dt.to_json(orient="records"))))
    print("OK!")

def insert_industry_members_info():
    index_col = [0, 1, 2, 3]
    long = 20880101
    df_columes = ["Industry", "Stock", "Date_in", "Date_out"]
    dt = pd.read_csv("industry_members.csv", encoding="gbk", usecols=index_col, names=df_columes)
    dt["Stock"] = dt["Stock"].map(stock_name_process)
    dt["Date_out"].fillna(long, inplace=True)
    dt["Date_out"] = dt["Date_out"].astype(int)
    industry_list = dt["Industry"].value_counts()
    # os.chdir("E:\\Git\\Data")
    # with open("Factors/industry", "w") as file:
    #     for i in industry_list.index:
    #         file.write(str(i)+"\n")
    # with open("Factors/industry_count", "w") as file:
    #     for i in industry_list.tolist():
    #         file.write(str(i)+"\n")
    dt["Industry"] = dt["Industry"].map(lambda x: industry_list.index.tolist().index(x))
    database_day["INDUSTRY_MARK"].insert_many(json.loads((dt.to_json(orient="records"))))
    print("OK!")

def insert_index_data():
    index_col = list(range(12))
    index_col.remove(2)
    df_columes = ["Stock", "Date", "preClose", "Open", "High", "Low", "Close", "Change", "PCTChange",
                           "Volume", "Value"]
    for year in range(2010, 2018):
        print(year)
        dt = pd.read_csv("index_market_data_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
        database_day["INDEX"].insert_many(json.loads((dt.to_json(orient="records"))))

def insert_market_data():
    index_col = list(range(12))
    index_col.remove(2)
    df_columes = ["Stock", "Date", "preClose", "Open", "High", "Low", "Close", "Change", "PCTChange",
                           "Volume", "Value"]
    for year in range(2010, 2018):
        print(year)
        dt = pd.read_csv("industry_market_data_%d.csv" %year, encoding="gbk", usecols=index_col, names=df_columes)
        database_day["INDUSTRY"].insert_many(json.loads((dt.to_json(orient="records"))))
    pass


if __name__ == "__main__":
    import os
    os.chdir("G:\\stock_data")
    # insert_wd_day_market_data()
    # insert_industry_members_info()
    # insert_derive_data()
    # insert_moneyflow_data()
    # insert_industry_members_info()
    # insert_index_data()
    insert_market_data()
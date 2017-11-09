# -*- coding:utf-8 -*-
import scipy.io as sio
import pickle
import pandas as pd
import numpy as np
import os
import pymongo as mg
import json
from load_data_new import load_day_data_from_wind

client = mg.MongoClient(host="166.111.17.78", port=27017)
database_min_fq = client["Stock_MIN_FQ"]
database_min    = client["Stock_MIN"]
database_day    = client["Stock_Day"]

def get_bar(data):
    if (data["Open"].iloc[0] / data["Open"].iloc[1] - 1.0) < 0.15:
        open = data["Open"].iloc[0]
    else:
        open = data["Open"].iloc[1]

    dt_high = data["High"].copy()
    dt_high.iloc[0] = 0.
    high  =  dt_high.max()

    dt_low = data["Low"].copy()
    dt_low[dt_low == 0.0 ] = 1000.
    low  =  dt_low.min()

    if data["Close"].iloc[-1] != 0.0:
        close  =  data["Close"].iloc[-1]
    else:
        close  =  data["Close"].iloc[-2]

    volume =  data["Volume"].sum()
    value  =  data["Value"].sum()
    limit  =  data["limit_mark"].iloc[0]
    return (open, high, low, close, volume, value, limit)

def check_with_wd(day_bar, day_data_wd_in, day, check_level=0.1):
    # print(day_bar)
    if abs(day_bar[3] - day_data_wd_in["Close"].iloc[0]) >= check_level:
        print("close price of %d does not match!"  %day)
        print("wd_close: %f" %day_data_wd_in["Close"].iloc[0])
        print("min_close: %f" %day_bar[3])
        return False
    if abs(day_bar[0] - day_data_wd_in["Open"].iloc[0]) >= (check_level*5):
        print("open price of %d does not match!" %day)
        print("wd_open: %f" %day_data_wd_in["Open"].iloc[0])
        print("min_open: %f" %day_bar[0])
        return True
    if abs(day_bar[4] * 0.01 / day_data_wd_in["Volume"].iloc[0] - 1) >= 0.3:
        print(abs(day_bar[4] *0.01  / day_data_wd_in["Volume"].iloc[0]  - 1))
        print("volume price of %d does not match!" %day)
        print("wd_vol: %.1f" %day_data_wd_in["Volume"].iloc[0])
        print("min_vol: %.1f" %(day_bar[4] * 0.01))
        return True
    if abs(day_bar[1] - day_data_wd_in["High"].iloc[0]) >= (check_level*5):
        print("high price of %d does not match!" %day)
        print("wd_high: %f" %day_data_wd_in["High"].iloc[0])
        print("min_high: %f" %day_bar[1])
        return True
    if abs(day_bar[2] - day_data_wd_in["Low"].iloc[0]) >= (check_level*5):
        print("low price of %d does not match!"  %day)
        print("wd_low: %f" %day_data_wd_in["Low"].iloc[0])
        print("min_low: %f" %day_bar[2])
        return True
    return True

def check_fq(day_bar, day, alpha, check_leve=0.3):
    if abs(day_bar[3] * alpha / close - 1.0) >= check_level:
        close_wd = day_data_wd[day_data_wd.Date == day]["Close"].iloc[0]
        alpha = close_wd / day_bar[3]
        print(day, alpha)
        return True
    else:
        return False

def insert_min_data(stocks_list=[]):
    if len(stocks_list) == 0:
        stocks_list = os.listdir(".")

    df_columns = ["ID", "Date", "Time", "Open", "High", "Low", "Close", "Volume", "Value"]
    for stock in stocks_list:
        stock_name = stock.split(".")[0]
        print(stock_name)
        load_factors = ["Stock", "Date", "Open", "High", "Low", "Close", "Volume", "Value", "ADJ",]
        day_data_wd = load_day_data_from_wind(stock_name, factor_list = load_factors)
        if len(day_data_wd) == 0:
            print("There is no day data of %s" %stock_name)
            continue

        df = pd.DataFrame(sio.loadmat(stock)["data1"], columns=df_columns)
        df["Stock"] = stock_name
        df["Date"] = df["Date"].astype("int")
        df["Mean"] = df["Value"] / df["Volume"]
        df.fillna(0, inplace=True)
        del df["ID"]


        trading_date = np.unique(df["Date"])
        trading_date = trading_date[trading_date > 20100101]
        # close = day_data_wd[day_data_wd.Date == trading_date[0]]["Close"].iloc[0] #get the open price from wind of first day for align
        alpha_0 = day_data_wd.sort_values(by="Date")["ADJ"].iloc[-1]

        for day in trading_date:
            if len(day_data_wd[day_data_wd.Date == day]) == 0:
                print("Date %d is not in wd" %day)
                continue
            year = "Y" + str(day)[:4]
            day_df = df[df.Date == day].copy()
            #mark limited trading day
            if day_df["Volume"][5:].sum() == 0.:
                day_df["limit_mark"] = 0
            else:
                day_df["limit_mark"] = 1
            #check volume:
            mean_price = day_df["Mean"][day_df["Mean"] != 0.].mean()
            if mean_price / day_df["Close"].iloc[-1] > 10.:
                day_df["Mean"]  /= 100.
                day_df["Volume"] *= 100.

            #Check FQ, align the close price
            day_bar = get_bar(day_df)  # (open, high, low, close, volume, value, limit)
            check_status = check_with_wd(day_bar, day_data_wd[day_data_wd.Date == day], day)
            if not check_status:
                beta = day_data_wd[day_data_wd.Date == day]["Close"].iloc[0] / day_bar[3]
                day_df[["Open", "High", "Low", "Close", "Mean"]] *= beta
            database_min[year].insert_many(json.loads((day_df.to_json(orient="records"))))

            alpha = day_data_wd[day_data_wd.Date == day]["ADJ"].iloc[0] / alpha_0
            day_df[["Open", "High", "Low", "Close", "Mean"]] *= alpha
            day_df[["Volume"]] /= alpha
            day_bar_changed = get_bar(day_df)

            database_min_fq[year].insert_many(json.loads((day_df.to_json(orient = "records"))))
            day_bar_insert = {"Date"  : day,
                              "Stock" : stock_name,
                              "Open"  : day_bar_changed[0],
                              "High"  : day_bar_changed[1],
                              "Low"   : day_bar_changed[2],
                              "Close" : day_bar_changed[3],
                              "Volume": day_bar_changed[4],
                              "Value" : day_bar_changed[5],
                              "Mean"  : day_bar_changed[5] * 1.0 / (day_bar_changed[4] + 1.0e-6),
                              "Limit" : day_bar_changed[6]}
            database_day["FMIN"].insert(day_bar_insert)
        with open("../Data/done_list.txt", "a") as file:
            file.write(stock_name+"\n")
    return

def insert_day_data():
    factor_list = ["dayOpen", "dayHigh", "dayLow", "dayClose", "dayVolume", "dayTurn", "daySwing"]
    for year in range(2007, 2018):
        print(year)
        dt = {}
        for f in factor_list:
            with open(f+str(year), "rb") as file:
                dt[f] = pickle.load(file)
        stock_list = dt["dayOpen"].Codes
        date_list  = dt["dayOpen"].Times
        for ind, stock in enumerate(stock_list):
            stock_name = stock.split(".")[0]
            df_insert = {"Date"  : date_list,
                         "Open"  : dt["dayOpen"].Data[ind],
                         "High"  : dt["dayHigh"].Data[ind],
                         "Low"   : dt["dayLow"].Data[ind],
                         "Close" : dt["dayClose"].Data[ind],
                         "Volume": dt["dayVolume"].Data[ind],
                         "Turn"  : dt["dayTurn"].Data[ind],
                         "Swing" : dt["daySwing"].Data[ind]}
            df_insert = pd.DataFrame(df_insert)
            df_insert["Stock"] = stock_name
            df_insert["Date"] = df_insert["Date"].apply(lambda x: int(x.strftime("%Y%m%d")))
            database_day["WIND"].insert_many(json.loads((df_insert.to_json(orient="records"))))
    return

if __name__ == "__main__":
    import os
    os.chdir("../MinData20170907")
    insert_day_data()
    # insert_min_data(["000800"])

# -*- coding:utf-8 -*-
import pandas as pd
import numpy as np
from global_variables import *
from Factors.factor_list import *

def get_date_cond(stock_name, start_date, end_date, index=True):
    if index:
        cond = {"Stock": stock_name}
        if start_date != None:
            cond["Date"] = {"$gte": start_date}
            if end_date != None:
                cond["Date"]["$lte"] = end_date
        elif end_date != None:
            cond["Date"] = {"$lte": end_date}
    else:
        cond = {"0": stock_name}
        if start_date != None:
            cond["1"] = {"$gte": start_date}
            if end_date != None:
                cond["1"]["$lte"] = end_date
        elif end_date != None:
            cond["1"] = {"$lte": end_date}
    return cond


def load_day_data(stock_name, start_date=None, end_date=None, factor_list=[], col="wd"):
    cond = get_date_cond(stock_name, start_date, end_date)

    show = {"_id": 0}
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1
        df_col = factor_list

    if col == "wd":
        querry = database_day["WIND_ALL"].find(cond, show)
        df_col = stock_market
    elif col == "fmin":
        querry = database_day["FMIN"].find(cond, show)
        df_col = stock_market_fmin
    elif col == "wd_fq":
        querry = database_day["WIND_FQ"].find(cond, show)
        df_col =stock_market_fq

    dt = pd.DataFrame(list(querry))
    dt = dt[df_col]
    dt = dt.sort_values(by="Date")
    return dt

def load_index_data(index, start_date=None, end_date=None, factor_list=[]):
    cond = get_date_cond(index, start_date, end_date)

    show = {"_id": 0}
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1
        df_col = factor_list
    else:
        df_col = index_market

    querry = database_day["INDEX"].find(cond, show)
    dt = pd.DataFrame(list(querry))
    dt = dt[df_col]
    dt = dt.sort_values(by="Date")
    return dt

def load_industry_data(index, start_date=None, end_date=None, factor_list=[]):
    cond = get_date_cond(index, start_date, end_date)

    show = {"_id": 0}
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1
        df_col = factor_list
    else:
        df_col = index_market

    querry = database_day["INDUSTRY"].find(cond, show)
    dt = pd.DataFrame(list(querry))
    dt = dt[df_col]
    dt = dt.sort_values(by="Date")
    return dt


def get_trading_date(start_date, end_date):
    dt = load_day_data("000001", start_date, end_date, factor_list=["Date"], col="wd_fq")
    return dt.values.reshape(-1).tolist()

def get_bar(data):
    if data["Open"].iloc[0] != 0.0:
        open = data["Open"].iloc[0]
    else:
        open = data["Open"].iloc[1]

    high  =  data["High"].max()

    dt_low = data["Low"].copy()
    dt_low[dt_low == 0.0 ] = 1000.
    low  =  dt_low.min()

    if data["Close"].iloc[-1] != 0.0:
        close  =  data["Close"].iloc[-1]
    else:
        close  =  data["Close"].iloc[-2]

    volume =  data["Volume"].sum()
    value  =  data["Value"].sum()
    return (open, high, low, close, volume, value)


def load_open_cost(stock, date, value, time=925., ratio=10.):
    year = str(date)[:-4]
    dc = database_min[year]
    feature_list = {"_id": 0, "Value": 1, "Volume": 1, "limit_mark": 1}
    show = dc.find({"Date": date, "Stock": stock,
                    "Time": {"$gte": time}}, feature_list)
    dt = pd.DataFrame(list(show))
    if dt["limit_mark"][0] == 0:
        return (0., 0.)
    val_total = 0.
    vol_total = 0.
    for ind, val in enumerate(dt["Value"]):
        val_total += val / ratio
        vol_total += dt["Volume"][ind] / ratio
        if val_total >= value:
            break
    return (val_total, vol_total)


def load_close_cost(stock, date, volume, time=925., ratio=10.):
    year = str(date)[:-4]
    dc = database_min[year]
    feature_list = {"_id": 0, "Value": 1, "Volume": 1, "limit_mark": 1}
    show = dc.find({"Date": date, "Stock": stock,
                    "Time": {"$gte": time}}, feature_list)
    dt = pd.DataFrame(list(show))
    if dt["limit_mark"][0] == 0:
        return (0., 0.)
    val_total = 0.
    vol_total = 0.
    for ind, val in enumerate(dt["Value"]):
        vol_total += dt["Volume"][ind] / ratio
        val_total += val / ratio
        if vol_total >= volume:
            delta = vol_total - volume
            val_total -= (val * delta / dt["Volume"][ind])
            vol_total = volume
            break
    return (val_total, vol_total)

def load_min_data(stock_name, start_date, end_date, factor_list=[], fq=True):
    start_year = int(start_date / 10000)
    end_year = int(end_date / 10000)
    cond = {"Stock": stock_name}
    cond["Date"] = {"$gte": start_date, "$lte": end_date}
    show = {"_id" : 0}
    dt = pd.DataFrame([])
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1
    for year in range(start_year, end_year + 1):
        if fq:
            querry = database_min_fq["Y"+str(year)].find(cond, show)

        else:
            querry = database_min["Y" + str(year)].find(cond, show)
        dt = pd.concat((dt, pd.DataFrame(list(querry))))
    dt = dt.set_index(np.arange(len(dt)))
    return dt

def load_derive_factors(stock_name, start_date=None, end_date=None, factor_list=[]):
    cond = get_date_cond(stock_name, start_date, end_date, index=False)

    show = {"_id" : 0}
    if len(factor_list) != 0:
        dt_col = []
        for f in factor_list:
            f_index = derive.index(f)
            show[str(f_index)] = 1
            dt_col.append(str(f_index))
        dt_columes = factor_list
    else:
        dt_columes = derive
        dt_col = [str(i) for i in range(len(dt_columes))]

    querry = database_day["DERIVE"].find(cond, show)
    dt = pd.DataFrame(list(querry))
    dt = dt[dt_col]
    dt.columns = dt_columes
    return dt

def load_money_flow_factors(stock_name, start_date=None, end_date=None, factor_list=[]):
    cond = get_date_cond(stock_name, start_date, end_date, index=False)

    show = {"_id" : 0}
    if len(factor_list) != 0:
        dt_col = []
        for f in factor_list:
            f_index = money_flow.index(f)
            show[str(f_index)] = 1
            dt_col.append(str(f_index))
        dt_columes = factor_list
    else:
        dt_columes = money_flow
        dt_col = [str(i) for i in range(len(dt_columes))]

    querry = database_day["MONEYFLOW"].find(cond, show)
    dt = pd.DataFrame(list(querry))
    dt = dt[dt_col]
    dt.columns = dt_columes
    return dt

def load_industry_mark(stock_name_list, start_date, end_date, industry_list):
    trading_date = get_trading_date(start_date, end_date)
    shape = (len(stock_name_list), len(trading_date), len(industry_list))
    df = np.zeros(shape=shape, dtype=int)  # (Stock, Date, Industry)
    show = {"_id" : 0}
    index_list = []
    for i in industry_list:
        if i not in industry:
            print("%s is not in industry list" %i)
            continue
        i_index = industry.index(i)
        index_list.append(i_index)
    cond = {"Stock" : {"$in" : stock_name_list}, "Industry" : {"$in" : index_list}}
    querry = database_day["INDUSTRY_MARK"].find(cond, show)
    dt = pd.DataFrame(list(querry))
    for ind in range(len(dt)):
        stock_ind  = stock_name_list.index(dt.loc[ind]["Stock"])
        indus_ind  = index_list.index(dt.loc[ind]["Industry"])
        inday  = dt.loc[ind]["Date_in"]
        outday = dt.loc[ind]["Date_out"]
        if (inday > end_date) or (outday < start_date):
            continue
        in_index = trading_date.index(max(inday, start_date))
        out_index = trading_date.index(min(outday, end_date))
        df[stock_ind, in_index: (out_index+1), indus_ind] = 1
    return df




if __name__ == "__main__":
    # load_open_cost("000001", 20070104, 932, )
    # mindata = load_min_data("600179", 20160101, 20160102, fq=True)
    # dt = load_day_data("600179", 20160315, 20160316, col="wd")
    # td = get_trading_date(20110301, 20110605)

    # factor_list = ["Stock", "Date", "S_VAL_PB_NEW", "S_VAL_PE_TTM", "S_VAL_PCF_OCF", "S_VAL_PCF_OCFTTM",
    #                     "S_VAL_PCF_NCF", "S_VAL_PCF_NCFTTM" ]
    # dt = load_derive_factors("600179", 20160315, 20160316, factor_list=factor_list)
    #
    # factor_list = ["Stock", "Date", "BUY_VALUE_EXLARGE_ORDER", "BUY_VALUE_MED_ORDER", "SELL_VALUE_EXLARGE_ORDER", "BUY_VALUE_LARGE_ORDER",
    #                   "SELL_VALUE_LARGE_ORDER",]
    # dt = load_money_flow_factors("600179", 20160315, 20160316,factor_list=factor_list)
    # load_industry_mark(stock_name_list=["600179", "000001"], start_date=20130201, end_date=20130301,
    #                    industry_list=["399985.SZ", "000985.CSI"])

    dt = load_industry_data("CI005178.WI", start_date=20150315, end_date=20150505,) #factor_list=["Stock", "Date", "preClose"])
    print("Start!")
    print("End")



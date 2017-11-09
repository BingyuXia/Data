import pymongo as mg
import pandas as pd
import numpy as np

client = mg.MongoClient(host="166.111.17.78", port=27017)
database_min_fq = client["Stock_MIN_FQ"]
database_min    = client["Stock_MIN"]
database_day    = client["Stock_Day"]

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


def load_day_data(stock_name, start_date=None, end_date=None, factor_list=[], col="wd"):
    cond = {"Stock": stock_name}
    if start_date != None:
        cond["Date"] = {"$gte": start_date}
        if end_date != None:
            cond["Date"]["$lte"] = end_date
    elif end_date != None:
        cond["Date"] = {"$lte": end_date}

    show = {"_id": 0}
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1
    if col == "wd":
        querry = database_day["WIND_ALL"].find(cond, show)
    elif col == "fmin":
        querry = database_day["FMIN"].find(cond, show)
    elif col == "wd_fq":
        querry = database_day["WIND_FQ"].find(cond, show)

    return pd.DataFrame(list(querry))


def load_min_data(stock_name, start_date, end_date, factor_list=[], fq=True):
    start_year = int(start_date / 10000)
    end_year = int(end_date / 10000)
    cond = {"Stock": stock_name}
    cond["Date"] = {"$gte": start_date, "$lte": end_date}
    show = {"_id": 0}
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


def bar_generation(bar_type, stock_name, start_date, end_date):
    min_data = load_min_data(stock_name, start_date, end_date)




if __name__ == "__main__":
    # load_open_cost("000001", 20070104, 932, )
    mindata = load_min_data("600179", 20160101, 20160102, fq=True)
    # day = load_day_data("600179", 20160315, 20160316, col="fmin")



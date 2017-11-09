import pymongo as mg
import pandas as pd

client = mg.MongoClient(host="166.111.17.78", port=27017)
database_min = client["Stock_MIN"]
database_day = client["Stock_Day"]

def get_bar(data):
    open   =  data["Open"][0]
    high   =  data["High"].max()
    low    =  data["Low"].min()
    close  =  data["Close"].iloc[-1]
    volume =  data["Volume"].sum()
    value  =  data["Value"].sum()
    return (open, high, low, close, volume, value)

def load_open_cost(stock, date, value, time=925., ratio=10.):
    if date < 20120101:
        db = client["stock_min_before12"]
    else:
        db = client["stock_min_before18"]
    feature_list = {"_id":0, "Value":1, "Volume":1, "limit_mark":1}
    show = db["T"+stock].find({"Date":date,
                               "Time": {"$gte":time}}, feature_list)
    dt = pd.DataFrame(list(show))
    if dt["limit_mark"][0] == 0:
        return (0., 0.)
    val_total = 0.
    vol_total = 0.
    for ind, val in enumerate(dt["Value"]):
        val_total += val / ratio
        vol_total += dt["Volume"][ind] / ratio
        if val_total >= value :
            break
    return (val_total, vol_total)

def load_close_cost(stock, date, volume, time=925., ratio=10.):
    if date < 20120101:
        db = client["stock_min_before12"]
    else:
        db = client["stock_min_before18"]
    feature_list = {"_id":0, "Value":1, "Volume":1, "Time":1, "limit_mark": 1}
    show = db["T"+stock].find({"Date":date, "Time":{"$gte":time}}, feature_list)
    dt = pd.DataFrame(list(show))
    if dt["limit_mark"][0] == 0:
        return (0., 0.)
    val_total = 0.
    vol_total = 0.
    for ind, val in enumerate(dt["Value"]):
        val_total += val / ratio
        vol_total += dt["Volume"][ind] / ratio
        if vol_total >= volume:
            break

    val_mean = val_total / vol_total
    vol_total = min(volume, vol_total)
    val_total = val_mean * vol_total
    return (val_total, vol_total)

def load_day_data_from_wind(stock_name, start_date=None, end_date=None, factor_list=[]):
    cond = {"Stock": stock_name}
    if start_date != None:
        cond["Date"] = {"$gte" : start_date}
        if end_date != None:
            cond["Date"]["$lte"] = end_date
    elif end_date != None:
        cond["Date"] = {"$lte" : end_date}

    show = {"_id" : 0}
    if len(factor_list) != 0:
        for f in factor_list:
            show[f] = 1

    querry = database_day["WIND"].find(cond, show)
    return pd.DataFrame(list(querry))

# def bar_generation(bar, stock_name, start_date=None, end_date=None):
#     if start_date is None:


if __name__ == "__main__":
    load_open_cost("000001", 20070104, 932,)
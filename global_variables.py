import pymongo as mg

client = mg.MongoClient(host="192.168.1.108", port=27017)
database_min_fq = client["Stock_MIN_FQ"]
database_min    = client["Stock_MIN"]
database_day    = client["Stock_Day"]


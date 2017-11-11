import pymongo as mg

client = mg.MongoClient(host="166.111.17.113", port=27017)
database_min_fq = client["Stock_MIN_FQ"]
database_min    = client["Stock_MIN"]
database_day    = client["Stock_Day"]

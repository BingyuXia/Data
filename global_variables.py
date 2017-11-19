import pymongo as mg
account = "znjy001"
pwd = "znpy2015"
client = mg.MongoClient(host="166.111.17.113", port=27017)
db = client['Stock_Day']
db.authenticate(account, pwd)
database_min_fq = client["Stock_MIN_FQ"]
database_min    = client["Stock_MIN"]
database_day    = client["Stock_Day"]


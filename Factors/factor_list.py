import os.path
path = os.path.split(__file__)[0]

with open(path + "\\derive", "r") as file:
    derive = file.read().splitlines()

with open(path + "\\money_flow", "r") as file:
    money_flow = file.read().splitlines()

with open(path + "\\industry", "r") as file:
    industry = file.read().splitlines()

with open(path + "\\stock_market", "r") as file:
    stock_market = file.read().splitlines()

with open(path + "\\stock_market_fq", "r") as file:
    stock_market_fq = file.read().splitlines()

with open(path + "\\stock_market_fmin", "r") as file:
    stock_market_fmin = file.read().splitlines()

with open(path + "\\index_market", "r") as file:
    index_market = file.read().splitlines()


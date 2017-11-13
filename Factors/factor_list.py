import os
pwd = os.getcwd()
with open(pwd + "\\Factors\\derive", "r") as file:
    derive = file.read().splitlines()

with open(pwd + "\\Factors\\money_flow", "r") as file:
    money_flow = file.read().splitlines()

with open(pwd + "\\Factors\\industry", "r") as file:
    industry = file.read().splitlines()

with open(pwd + "\\Factors\\stock_market", "r") as file:
    stock_market = file.read().splitlines()

with open(pwd + "\\Factors\\stock_market_fq", "r") as file:
    stock_market_fq = file.read().splitlines()

with open(pwd + "\\Factors\\stock_market_fmin", "r") as file:
    stock_market_fmin = file.read().splitlines()

with open(pwd + "\\Factors\\index_market", "r") as file:
    index_market = file.read().splitlines()


print("OK")
from get_stock_price import *
import pandas as pd
import pykrx
test = GetStockPrice()
# ddf2 = pd.read_csv("csvFile/stockPrice.csv")
# ddf = stock.get_market_ohlcv_by_date("20180109", "20220101", "032790", adjusted=True).reset_index()
# ddf = ddf.astype(object)
# ddf.loc[ddf["시가"] ==0, "시가"] = ddf["종가"]
# print(ddf[ddf["open"]==0])
# print(ddf.info)
# print(ddf2[ddf2["시가"]==0])
# print(df.duplicated(keep='first').sum())
df = test.read_stock_price()
print(df[df["회사명"]=="BNGT"])

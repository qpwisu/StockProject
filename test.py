from get_stock_price import *
import pandas as pd
import pykrx
test = GetStockPrice()
ddf2 = pd.read_csv("csvFile/stockPrice.csv")
print(ddf2.groupby("티커").count())

tickers = stock.get_market_ticker_list("20220501", market="ALL")
print(len(tickers))
# print(df.duplicated(keep='first').sum())

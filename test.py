from get_stock_price import *
import pandas as pd
import pykrx
test = GetStockPrice()
ddf = stock.get_market_ohlcv_by_date("20180401", "20220630", "377300", adjusted=True)

print(ddf)

# df = pd.read_csv("csvFile/stockPrice.csv")
# print(df.duplicated(keep='first').sum())

import pymysql
from sqlalchemy import create_engine
import pandas as pd
from get_stock_price import *
class UploadMysql():
    def __init__(self):
        self.engine = create_engine("mysql+pymysql://root:" + "1234" + "@localhost/stock", encoding='utf-8')
        self.gsp = GetStockPrice()
    # def upload_price(self,df):
    #     df.columns = ["ticker", "open", "high", "low", "end", "volume", "mount", "rate", "day", "name"]
    #     df.to_sql(name='price', con=self.engine, if_exists='append', index=False)

    def first_mysql_upload_stock_price(self,df):

        df.columns = ["ticker", "open", "high", "low", "end", "mount", "day", "name"]
        df.to_sql(name='price', con=self.engine, if_exists='append', index=False)

    def update_mysql_upload_stock_price(self,append_df):
        if append_df.empty:
            return
        print(append_df)
        append_df.columns = ["ticker", "open", "high", "low", "end", "mount", "day", "name"]
        append_df.to_sql(name='price', con=self.engine, if_exists='append', index=False)

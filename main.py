from get_stock_price import *
from upload_mysql import *
import pandas as pd
gsp = GetStockPrice()
um = UploadMysql()

def first(start_day,end_day): #"20220101", "20221010"
    #주가 csv파일로 저장
    gsp.multiThread_get_stock_price(start_day,end_day)
    #stockprice.csv 파일 읽기
    df = gsp.read_stock_price()
    #mysql에 stockprice.csv 첫 업로드
    um.first_mysql_upload_stock_price(df)
#전에 업로드한 날짜부터 today -1 까지의 주가 업데이트
def update():
    #apppendStockPrice에 새로 추가될 주가 저장하고 stockPrice와 concat
    gsp.update_stock_price()
    #apppendStockPrice.csv 파일 읽기
    df=gsp.read_append_stock_price()
    #기존 table에 새로 추가된 주가 추가
    um.update_mysql_upload_stock_price(df)

first("20180101", "20220501")
update()


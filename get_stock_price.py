
from datetime import datetime, timedelta
from concurrent import futures
from pykrx import stock
import pandas as pd
import time
class GetStockPrice():
    #start_day ~ end_day까지의 날짜 가격 구함
    #8배 정도의 속도 향상
    def multiThread_get_stock_price(self,start_day, end_day):
        print("start get_stock_price")
        start = time.time()  # 시작 시간 저장

        global df
        df = pd.DataFrame(columns=['티커', '시가', '고가', '저가', '종가', '거래량', '거래대금', '등락률', 'day', '회사명'])
        def str_day(d):
            return d.strftime('%Y%m%d')

        # 입력한 기간동안에 개장일을 구하기
        tmpDays = stock.get_market_ohlcv(start_day, end_day, "005930")
        days = list(map(str_day, tmpDays.index.to_list()))
        if len(days)<=1:
            print(start_day,end_day)
            print("업데이트할 내용이 없습니다 ")
            return df

        # 날짜 리스트를 받아서 각각의 쓰레드가 전역 df에 결과값들을 저장
        def get_stock(days):
            for day in days:
                ddf = stock.get_market_ohlcv(day, market="ALL").reset_index()
                ddf["day"] = day
                global df
                df=pd.concat([df,ddf])

        #멀티 쓰레드 처리
        #날짜를 나눠서 쓰레드별로 처리
        def list_chunk(lst, n):
            return [lst[i:i + len(lst)//n] for i in range(0, len(lst), len(lst)//n)]
       #사용할 쓰레드 갯수 지정
        num_thread = 10
        if len(days)<10:
            num_thread = len(days)

        with futures.ThreadPoolExecutor() as executor:
            sub_routine = list_chunk(days,num_thread)
            results = executor.map(get_stock, sub_routine)
        #티커를 이용해서 회사명 열 추가 멀티 쓰레드 사용
        def ticker_change_name(tickers):
            for ticker in tickers:
                name = stock.get_market_ticker_name(ticker)
                global df
                df.loc[df["티커"] == ticker, "회사명"] = name

        tickerList = df.groupby("티커").size().index

        with futures.ThreadPoolExecutor() as executor:
            sub_routine = list_chunk(tickerList,num_thread)
            results = executor.map(ticker_change_name, sub_routine)
        print("멀티쓰레드 getstockprice time :", time.time() - start)  # 현재시각 - 시작시간 =드실행 시간
        self.save_price_csv(df)
        return df
    #기존에 구한 날짜 +1 ~ 오늘 -1 날짜 사이에 가격을 구함
    def update_stock_price(self):
        pre_df = pd.read_csv("csvFile/stockPrice.csv")
        start_day = (datetime.strptime(str(pre_df.day.max()),"%Y%m%d")+timedelta(days=1)).strftime("%Y%m%d")
        last_date = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
        print(start_day, last_date)
        # if last_date <= start_day:
        #     print("Update할 정보가 없습니다")
        #     return None
        aft_df = self.multiThread_get_stock_price(start_day,last_date)
        if aft_df.empty:
            aft_df.to_csv("csvFile/appendStockPrice.csv", mode='w', index=False)
            return aft_df
        df = pd.concat([pre_df,aft_df])
        aft_df.to_csv("csvFile/appendStockPrice.csv", mode='w',index=False)
        self.save_price_csv(df)
        return df
    def save_price_csv(self,df):
        #회사명, day 기준 오름차순 sort
        df.sort_values(["회사명", "day"], inplace=True)
        #중복값 제거
        df.drop_duplicates(inplace=True)
        df.to_csv("csvFile/stockPrice.csv", mode='w',index=False)

    def read_stock_price(self):
        df = pd.read_csv("csvFile/StockPrice.csv")
        return df

    def read_append_stock_price(self):
        df = pd.read_csv("csvFile/appendStockPrice.csv")
        return df

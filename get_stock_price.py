import threading
from datetime import datetime, timedelta
from concurrent import futures
from pykrx import stock
import pandas as pd
from time import time
from tqdm import tqdm


class GetStockPrice():
    #이거는 액면 분할되기 전값이 그대로 들어가있어서 데이터 사용하기 힘듬 stock.get_market_ohlcv 사용해야함
    #start_day ~ end_day까지의 날짜 가격 구함
    #8배 정도의 속도 향상
    # def multiThread_get_stock_price(self,start_day, end_day):
    #     print("start get_stock_price")
    #     start = time.time()  # 시작 시간 저장
    #
    #     global df
    #     df = pd.DataFrame(columns=['티커', '시가', '고가', '저가', '종가', '거래량', '거래대금', '등락률', 'day', '회사명'])
    #     def str_day(d):
    #         return d.strftime('%Y%m%d')
    #
    #     # 입력한 기간동안에 개장일을 구하기
    #     tmpDays = stock.get_market_ohlcv(start_day, end_day, "005930")
    #     days = list(map(str_day, tmpDays.index.to_list()))
    #     if len(days)<=1:
    #         print(start_day,end_day)
    #         print("업데이트할 내용이 없습니다 ")
    #         return df
    #
    #     # 날짜 리스트를 받아서 각각의 쓰레드가 전역 df에 결과값들을 저장
    #     def get_stock(days):
    #         for day in days:
    #             ddf = stock.get_market_ohlcv(day, market="ALL").reset_index()
    #             ddf["day"] = day
    #             global df
    #             df=pd.concat([df,ddf])
    #
    #     #멀티 쓰레드 처리
    #     #날짜를 나눠서 쓰레드별로 처리
    #     def list_chunk(lst, n):
    #         return [lst[i:i + len(lst)//n] for i in range(0, len(lst), len(lst)//n)]
    #    #사용할 쓰레드 갯수 지정
    #     num_thread = 10
    #     if len(days)<10:
    #         num_thread = len(days)
    #
    #     with futures.ThreadPoolExecutor() as executor:
    #         sub_routine = list_chunk(days,num_thread)
    #         results = executor.map(get_stock, sub_routine)
    #     #티커를 이용해서 회사명 열 추가 멀티 쓰레드 사용
    #     def ticker_change_name(tickers):
    #         for ticker in tickers:
    #             name = stock.get_market_ticker_name(ticker)
    #             global df
    #             df.loc[df["티커"] == ticker, "회사명"] = name
    #
    #     tickerList = df.groupby("티커").size().index
    #
    #     with futures.ThreadPoolExecutor() as executor:
    #         sub_routine = list_chunk(tickerList,num_thread)
    #         results = executor.map(ticker_change_name, sub_routine)
    #     print("멀티쓰레드 getstockprice time :", time.time() - start)  # 현재시각 - 시작시간 =드실행 시간
    #     self.save_price_csv(df)
    #     return df

    #위에는 액면 분할되기 전값이 그대로 들어가있어서 데이터 사용하기 힘듬 stock.get_market_ohlcv 사용해야함
    def multiThread_get_stock_price(self,start_day, end_day):
        lock = threading.Lock()

        print("start get_stock_price")
        start = time.time()  # 시작 시간 저장
        global df
        df = pd.DataFrame(columns=['티커', '시가', '고가', '저가', '종가', '거래량', '날짜', '회사명'])
        tickers = stock.get_market_ticker_list(end_day, market="ALL")

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
        def get_stock(tickers):
            tmp = pd.DataFrame(columns=['티커', '시가', '고가', '저가', '종가', '거래량', '날짜', '회사명'])
            #tqdm 진행표를 쓰레드 한개만 보기위해서 쓰레드 이름으로 구별
            threadName = threading.currentThread().getName()
            if threadName== "ThreadPoolExecutor-0_0":
                for ticker in tqdm(tickers):
                    ddf = stock.get_market_ohlcv_by_date(start_day, end_day, ticker, adjusted=True).reset_index()
                    ddf["티커"] = ticker
                    ddf["날짜"] = ddf["날짜"].dt.strftime("%Y%m%d")
                    name = stock.get_market_ticker_name(ticker)
                    ddf["회사명"] = name
                    tmp = pd.concat([tmp, ddf])
            else:
                for ticker in tickers:
                    ddf = stock.get_market_ohlcv_by_date(start_day, end_day, ticker, adjusted=True).reset_index()
                    ddf["티커"] = ticker
                    ddf["날짜"] = ddf["날짜"].dt.strftime("%Y%m%d")
                    name = stock.get_market_ticker_name(ticker)
                    ddf["회사명"] = name
                    tmp = pd.concat([tmp, ddf])
            global df
            # df라는 전역변수에 쓰레드들이 동시 접근하여 누락되는 값이 생김 공유 자원에 lock을 걸어서 한번에 한쓰레드만 사용가능하게 끔함
            # lock = threading.Lock()
            lock.acquire()
            df=pd.concat([df,tmp])
            lock.release()


        #멀티 쓰레드 처리
        #날짜를 나눠서 쓰레드별로 처리
        def list_chunk(lst, n):
            return [lst[i:i + len(lst)//n] for i in range(0, len(lst), len(lst)//n)]
       #사용할 쓰레드 갯수 지정
        num_thread = 8
        # if len(days)<10:
        #     num_thread = len(days)

        with futures.ThreadPoolExecutor() as executor:
            sub_routine = list_chunk(tickers,num_thread)
            results = executor.map(get_stock, sub_routine)
        #액면분할시 일정 기간동안 시가,고가,종가가 0으로 나온다 이를 종가로 통일
        print("멀티쓰레드 getstockprice time :", time.time() - start)  # 현재시각 - 시작시간 =드실행 시간
        self.save_price_csv(df)
        return df
    #기존에 구한 날짜 +1 ~ 오늘 -1 날짜 사이에 가격을 구함
    def update_stock_price(self):
        pre_df = pd.read_csv("csvFile/stockPrice.csv")
        start_day = (datetime.strptime(str(pre_df.날짜.max()),"%Y%m%d")+timedelta(days=1)).strftime("%Y%m%d")
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
        df.sort_values(["회사명", "날짜"], inplace=True)
        #원텍이라는 종목의 티커가 2개있음 하나는 상장 폐지 된거
        if 216280 in (df["티커"].values):
            df.loc[df["티커"] == 216280, "회사명"] = "원텍konex"
        #중복값 제거
        df.drop_duplicates(inplace=True)
        df.to_csv("csvFile/stockPrice.csv", mode='w',index=False)

    def read_stock_price(self):
        df = pd.read_csv("csvFile/StockPrice.csv")
        df = self.delZero(df)
        return df

    def read_append_stock_price(self):
        df = pd.read_csv("csvFile/appendStockPrice.csv")
        df = self.delZero(df)
        return df

    def delZero(self,df):
        # 시,고,저가 0인 종목들 종가로 통일 ex) 액면분할 기간
        df.loc[df["시가"] == 0, "시가"] = df["종가"]
        df.loc[df["고가"] == 0, "고가"] = df["종가"]
        df.loc[df["저가"] == 0, "저가"] = df["종가"]
        return df


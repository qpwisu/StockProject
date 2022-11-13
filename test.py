from get_stock_price import *
import pandas as pd
from concurrent import futures
from tqdm import tqdm
test = GetStockPrice()

df = pd.read_csv("csvFile/stock.csv",index_col=0).sort_index().iloc[0:100,:]
print(df)
df= df.fillna(0)
cor = df.corr(method='pearson').round(3)
corr_dic = cor.to_dict("series")
corr_df= pd.DataFrame(columns=["종목명","비교종목명","상관계수"])
for comp in tqdm(corr_dic.keys()):
    tmp_df = corr_dic[comp].to_frame().reset_index()
    tmp_df["종목명"] = comp
    tmp_df.columns = ["비교종목명", "상관계수", "종목명"]
    tmp_df = tmp_df[["종목명", "비교종목명", "상관계수"]]
    corr_df= pd.concat([corr_df,tmp_df])
print(corr_df)
# c=cor.pivot(index="날짜",columns="회사명",values="종가")

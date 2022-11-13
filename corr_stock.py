import itertools

import pandas as pd
from tqdm import tqdm
class corr():
    def change_df(self,df):
        print("start chage_df")
        corr_df = df.pivot(index="날짜", columns="회사명", values="종가")
        return corr_df
    # def save_corr_df(self,df):
    #     df.to_csv("csvFile/stock.csv", mode='w')
    #
    # def update_change_df(self):
    #     tmp_df = pd.read_csv("csvFile/appendStockPrice.csv")
    #     tmp_df2 = pd.read_csv("csvFile/stock.csv",index_col=0).sort_index()
    #     append_df = self.change_df(tmp_df)
    #     print(len(tmp_df2.columns),len(append_df.columns))
    #     print(pd.concat([tmp_df2, append_df]))

    def correlation(self,df):
        print("start correlation")
        df = df.fillna(0).sort_index()
        df100 = df.iloc[-100:, :]
        df200 = df.iloc[-200:, :]
        df300 = df.iloc[-300:, :]
        cor100 = df100.corr(method='pearson').round(3).astype(str)
        cor200 = df200.corr(method='pearson').round(3).astype(str)
        cor300 = df300.corr(method='pearson').round(3).astype(str)
        # cor300 = df300.corr(method='pearson').round(3).to_dict("series")
        cor = cor100+"/"+cor200+"/"+cor300
        #df을 dict로 변환 / 2중 dict 구조 {index:{colume:corr},index:{colume:corr},index:{colume:corr}.....}
        tmp =cor.to_dict()

        # 1차원에 있는 dict를 zip로 합쳐서 리스트로 변환
        tmp2 = list(map(lambda x: list(zip(x.keys(),x.values())),tmp.values()))
        #2차원 dict의 key값과 tmp2 리스트를 zip으로 매칭
        tmp3= list(zip(tmp.keys(),tmp2))
        # 2중으로 람다 사용해서 각 리스와 index 매칭
        tmp4 = list(map(lambda x: list(map(lambda y : [x[0]]+list(y),x[1])),tmp3))
        #차원 축소
        tmp5 = list(itertools.chain(*tmp4))
        # corr100,corr200,corr300 분할하고 형변환
        tmp6 = list(map(lambda x: [x[0],x[1]]+ list(map(float,(x[2].split("/")))),tmp5))
        corr_df = pd.DataFrame(tmp6, columns=["name1", "name2", "corr100","corr200","corr300"])
        corr_df.fillna(0, inplace=True)
        return corr_df

    def get_corr(self,df):
        df_c = self.change_df(df)
        df_corr= self.correlation(df_c)
        return df_corr

        # print(list(zip(tmp.keys(),list(zip(tmp.values().keys(), tmp.values().values())))))

        # corr_df = pd.DataFrame(columns=["종목명", "비교종목명", "corr100","corr200","corr300"])
        # # cor = df.corr(method='pearson').round(3)
        # for comp in tqdm(df.columns):
        #     tmp_df = cor[comp].to_frame().reset_index()
        #     tmp_df["종목명"] = comp
        #     tmp_df.columns = ["비교종목명", "상관계수", "종목명"]
        #     tmp_df['corr100'] = tmp_df.상관계수.str.split('/').str[0]
        #     tmp_df['corr200'] = tmp_df.상관계수.str.split('/').str[1]
        #     tmp_df['corr300'] = tmp_df.상관계수.str.split('/').str[2]
        #     tmp_df.drop(['상관계수'], axis=1,inplace =True)
        #     tmp_df = tmp_df[["종목명", "비교종목명", "corr100","corr200","corr300"]]
        #     # tmp_df.astype({'corr100': 'float','corr200': 'float','corr300': 'float'})
        #     corr_df = pd.concat([corr_df, tmp_df])
        # df.to_csv("csvFile/stockCorr.csv", mode='w')

# print(df)
# df.to_csv("csvFile/stock.csv", mode='w')
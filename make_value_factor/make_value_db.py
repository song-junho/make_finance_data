from selenium import webdriver
import pyperclip
import time
from selenium.webdriver.common.keys import Keys
import pandas as pd
from tqdm import tqdm
import numpy as np
import time
import pickle
import datetime
from dateutil.relativedelta import relativedelta
import FinanceDataReader as fdr
from sqlalchemy import create_engine, Column, Integer, String, text

from sqlalchemy import create_engine
import pymysql

import time
import threading
import multiprocessing
import concurrent.futures



class MakeValueDB():


    def __init__(self):
        """
        :param term_typ: {q: 분기 , y: 연간, ttm: 4분기 합산}
        """

        pymysql.install_as_MySQLdb()

        self.user_nm = "root"
        self.user_pw = "ss019396"

        self.host_nm = "127.0.0.1:3306"

        engine = create_engine("mysql+mysqldb://" + self.user_nm + ":" + self.user_pw + "@" + self.host_nm)

        conn = engine.connect()

        # 재무데이터 마스터
        self.df_fin_master = pd.read_sql_query(text('SELECT * FROM financial_data.finance_master'), conn)

        # 가격 Dictionary 생성
        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        # 재무데이터 (TTM)
        list_item_cd = (121000, 121200, 121500, 122700, 123000, 115000, 315000)
        q = 'SELECT * FROM financial_data.financial_statement_ttm' + ' where item_cd in {} and freq = "yoy"'.format(
            list_item_cd)
        q = text(q)
        self.df_fin_ttm = pd.read_sql_query(q, conn).sort_values(['cmp_cd', 'yymm', 'fin_typ', 'freq']).drop_duplicates(
            ["term_typ", "cmp_cd", "item_cd", "yymm", "freq"], keep="last").reset_index(drop=True)

        # 재무데이터 (Y)
        list_item_cd = 423400
        q = 'SELECT * FROM financial_data.financial_statement_y' + ' where item_cd = {} and freq = "yoy"'.format(
            list_item_cd)
        q = text(q)
        self.df_fin_y = pd.read_sql_query(q, conn).sort_values(['cmp_cd', 'yymm', 'fin_typ', 'freq']).drop_duplicates(
            ["term_typ", "cmp_cd", "item_cd", "yymm", "freq"], keep="last").reset_index(drop=True)

    def get_df_stock(self, cmp_cd, item_cd, df):

        '''
        전처리 yymm -> date
        yoy 데이터로 축소
        '''

        df_res = df[(df["cmp_cd"] == cmp_cd) & (df["item_cd"] == item_cd)]
        df_res = df_res[df_res["freq"] == "yoy"]
        df_res["yymm"] = pd.to_datetime(
            df_res["yymm"].astype("str").str[:4] + "-" + df_res["yymm"].astype("str").str[-2:] + '-01')

        return df_res

    def set_announce_date(self, df):

        '''
        재무일자 -> 발표일자 변환

        재무(월)


        3월  ->  5월 20일
        6월  ->  8월 20일
        9월  -> 11월 20일
        12월 ->  3월 20일(일반적으로 잠정이 더 앞서 발표하나, 보수적으로 3월 20일 )

        12월 재무데이터 이외는 모두 +2개월 & 20일

        '''

        df_res = df.copy()

        for num in range(0, len(df_res)):

            yymm = df_res.loc[num, "yymm"]

            year = yymm.year
            month = yymm.month
            day = yymm.day

            if month == 12:

                year += 1
                month = 3
                day = 20

            elif month >= 11:

                year += 1
                month -= 10
                day = 20

            else:

                month += 2
                day = 20

            df_res.loc[num, "yymm"] = datetime.datetime(year, month, day)

        return df_res


    def multiple_data(self, list_cmp_cd):

        '''
        멀티플 산정 실적

        1. 멀티플 지표

            PSR           = 900001
            PGR           = 900002
            POR           = 900003
            PER           = 900004
            PBR           = 900005
            PCR           = 900006
            EV/EBITDA     = 900007
            시가배당률(%) = 900008

        2. 멀티플 분모

            매출액       (121000)
            매출총이익   (121200)
            영업이익     (121500)
            당기순이익   (122700)
            자본총계     (115000)
            영업현금흐름(* 따로 데이터가 없으므로, 주가/CFPS로 연산), (315000)
            EBITDA       (123000)
            DPS          (423400, DPS/주가 로 연산)

        '''

        multiple_keys = {
            121000: 900001,
            121200: 900002,
            121500: 900003,
            122700: 900004,
            115000: 900005,
            315000: 900006,
            123000: 900007,
            #     423400: 900008  ,

        }

        list_df_res = []
        list_err_cd = []

        for cmp_cd in tqdm(list_cmp_cd):

            try:
                df_res = pd.DataFrame()

                df_fin_ttm_tmp = self.df_fin_ttm[self.df_fin_ttm["cmp_cd"] == cmp_cd]
                for item_cd in multiple_keys.keys():

                    df_stock = self.get_df_stock(cmp_cd, item_cd, df_fin_ttm_tmp).reset_index(drop=True)
                    df_stock = self.set_announce_date(df_stock).reset_index(drop=True)
                    df_stock = df_stock.set_index("yymm")

                    df_price = self.dict_df_stock[cmp_cd]

                    df_merge = \
                    pd.merge(left=df_stock, right=df_price, how="outer", left_index=True, right_index=True).ffill()[
                        ["val", "MarketCap", "Close"]]

                    df_merge = df_merge[df_merge["MarketCap"] > 0]
                    df_merge = df_merge.fillna(0)

                    df_merge["MarketCap"] = (df_merge["MarketCap"] / 100000000).astype("int64")

                    # PCR
                    if item_cd == 315000:
                        df_merge["multiple"] = df_merge["Close"] / df_merge["val"]

                    # 시가배당률(%)
                    elif item_cd == 423400:
                        df_merge["multiple"] = (df_merge["val"] / df_merge["Close"]) * 100

                    # 일반 멀티플
                    else:
                        df_merge["multiple"] = df_merge["MarketCap"] / df_merge["val"]

                    df_merge["item_cd"] = multiple_keys[item_cd]
                    df_merge["cmp_cd"] = cmp_cd

                    df_merge.loc[df_merge["multiple"] == np.inf, "multiple"] = 0

                    df_merge = df_merge.loc[datetime.datetime(2000, 1, 1):][["cmp_cd", "item_cd", "val", "multiple"]]

                    df_res = pd.concat([df_res, df_merge])

            except:
                list_err_cd.append(cmp_cd)
                continue

            list_df_res.append(df_res)

        return list_df_res

    def dividen_data(self, list_cmp_cd):
        # 시가배당률(%) 은 연간 재무제표로 다시 산정해야함

        multiple_keys = {
            423400: 900008,

        }
        list_df_res = []
        list_err_cd = []

        for cmp_cd in tqdm(list_cmp_cd):

            try:
                df_res = pd.DataFrame()

                df_fin_y_tmp = self.df_fin_y[self.df_fin_y["cmp_cd"] == cmp_cd]
                for item_cd in multiple_keys.keys():
                    df_stock = self.get_df_stock(cmp_cd, item_cd, df_fin_y_tmp).reset_index(drop=True)
                    df_stock = self.set_announce_date(df_stock).reset_index(drop=True)
                    df_stock = df_stock.set_index("yymm")

                    df_price = self.dict_df_stock[cmp_cd]

                    df_merge = \
                    pd.merge(left=df_stock, right=df_price, how="outer", left_index=True, right_index=True).ffill()[
                        ["val", "MarketCap", "Close"]]

                    df_merge = df_merge[df_merge["MarketCap"] > 0]
                    df_merge = df_merge.fillna(0)

                    df_merge["MarketCap"] = (df_merge["MarketCap"] / 100000000).astype("int64")

                    df_merge["multiple"] = (df_merge["val"] / df_merge["Close"]) * 100

                    df_merge["item_cd"] = multiple_keys[item_cd]
                    df_merge["cmp_cd"] = cmp_cd

                    df_merge.loc[df_merge["multiple"] == np.inf, "multiple"] = 0

                    df_merge = df_merge.loc[datetime.datetime(2000, 1, 1):][["cmp_cd", "item_cd", "val", "multiple"]]

                    df_res = pd.concat([df_res, df_merge])

            except:
                list_err_cd.append(cmp_cd)
                continue

            list_df_res.append(df_res)

        return list_df_res

    def run(self):

        list_thread = []
        #  전체 종목 100개 단위 분할, 약 23개 스레드
        n = 600
        list_cmp_cd_t = self.df_fin_ttm["cmp_cd"].unique()
        list_cmp_cd_t = [list_cmp_cd_t[i * n:(i + 1) * n] for i in range((len(list_cmp_cd_t) + n - 1) // n)]

        pool = concurrent.futures.ProcessPoolExecutor(max_workers=4)

        start = time.time()

        list_df_res = []
        for i, list_cmp_cd in enumerate(list_cmp_cd_t):
            list_thread.append(pool.submit(self.multiple_data, list_cmp_cd))

        for t in concurrent.futures.as_completed(list_thread):
            list_df_res.extend(t.result())

        list_df_res.extend(self.dividen_data(list_cmp_cd_t))
        end = time.time()
        print(end - start)


        # 멀티플 데이터 생성
        list_tmp = []

        df_tmp = pd.DataFrame()
        for num in tqdm(range(0, len(list_df_res))):

            df_tmp = pd.concat([df_tmp, list_df_res[num]])

            if (num % 100 == 0) & (num != 0):
                list_tmp.append(df_tmp)
                df_tmp = pd.DataFrame()  # 초기화

        list_tmp.append(df_tmp)

        df_multiple = pd.DataFrame()
        for num in tqdm(range(0, len(list_tmp))):
            df_multiple = pd.concat([df_multiple, list_tmp[num]])

        df_multiple = df_multiple.reset_index()
        df_multiple = df_multiple.rename(columns={"index": "date"})

        df_multiple["item_cd"] = df_multiple["item_cd"].astype("int32")
        df_multiple["val"] = df_multiple["val"].astype("int32")
        df_multiple["cmp_cd"] = df_multiple["cmp_cd"].astype('category')
        df_multiple["multiple"] = df_multiple["multiple"].astype("float32")

        df_multiple = df_multiple.sort_values(["cmp_cd", "item_cd", "date"])
        df_multiple = df_multiple.reset_index(drop=True)

        # 메모리 해제
        del [[list_tmp]]
        del [[list_df_res]]

        # 기존 데이터 백업
        file_name = 'df_multiple_' + datetime.datetime.today().strftime("%Y%m%d")

        with open(r'D:\MyProject\종목분석_환경\multiple_DB\df_multiple.pickle', 'rb') as fr:
            df_backup = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(df_backup, fw)
                del [[df_backup]]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\df_multiple.pickle', 'wb') as fw:
            pickle.dump(df_multiple, fw)

        # 1. key-value(cmp_cd)
        dict_multiple_cmp_cd = {}

        for cmp_cd in tqdm(df_multiple["cmp_cd"].unique()):
            dict_multiple_cmp_cd[cmp_cd] = df_multiple[df_multiple["cmp_cd"] == cmp_cd]

        # 2. key-value(date)
        dict_multiple_date = {}

        for v_date in tqdm(pd.to_datetime(df_multiple["date"].unique())):
            dict_multiple_date[v_date] = df_multiple[df_multiple["date"] == v_date]

        # 1. key-value(cmp_cd)

        # 기존 데이터 백업
        file_name = 'dict_multiple_cmp_cd' + datetime.datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_cmp_cd.pickle', 'rb') as fr:
            back_up = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(back_up, fw)
                del [back_up]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_cmp_cd.pickle', 'wb') as fw:
            pickle.dump(dict_multiple_cmp_cd, fw)

        # 2. key-value(date)

        # 기존 데이터 백업
        file_name = 'dict_multiple_date' + datetime.datetime.today().strftime("%Y%m%d")
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_date.pickle', 'rb') as fr:
            back_up = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(back_up, fw)
                del [back_up]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_date.pickle', 'wb') as fw:
            pickle.dump(dict_multiple_date, fw)

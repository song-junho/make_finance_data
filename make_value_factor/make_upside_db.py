from config import API_KEY
import pandas as pd
from tqdm import tqdm
import numpy as np
import pickle
import datetime

from sqlalchemy import text

from sqlalchemy import create_engine
import pymysql

import time
from collections import deque
import concurrent.futures
import math



class MakeUpsideDB():


    def __init__(self):
        """
        :param term_typ: {q: 분기 , y: 연간, ttm: 4분기 합산}
        """

        pymysql.install_as_MySQLdb()

        self.user_nm = API_KEY["MYSQL"]["ID"]
        self.user_pw = API_KEY["MYSQL"]["PW"]

        self.host_nm = API_KEY["MYSQL"]["HOST"]

        engine = create_engine("mysql+mysqldb://" + self.user_nm + ":" + self.user_pw + "@" + self.host_nm)

        conn = engine.connect()

        # 재무데이터 마스터
        self.df_fin_master = pd.read_sql_query(text('SELECT * FROM financial_data.finance_master'), conn)

        # 가격 Dictionary 생성
        with open(r"D:\MyProject\StockPrice\DictDfStock.pickle", 'rb') as fr:
            self.dict_df_stock = pickle.load(fr)

        # 1. key-value(cmp_cd)
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_cmp_cd.pickle', 'rb') as fr:
            self.dict_multiple_cmp_cd = pickle.load(fr)

    def make_column(self, cmp_cd, item_cd, value_q):
        '''
        history_multiple 생성
        upside 생성

        - 전처리

            history_multiple => 중앙값
            최근 3~4년치


            연산에서 제거되는 데이터 ->
             1. 0 이하의 multiple
             2. 구간내 중앙값 보다 5배 이상 높은 multiple

            0 이하의 multiple 수치는 제외해서 연산 -> dataframe에서 제외한 후, rolling을  제외된 구간은 NaN 처리 될 것
            dataframe에서 제외한 후, rolling
            rolling 후 변경 데이터 이전의 값과 merge (* 제거돼 비어있는 날짜를 채우기 위함)
            이때, NaN 값이 생기는 history_multiple 구간은 ffill을 사용해 직전 적정값(history_multiple)으로 채워넣는다
            upside 연산 시, 0 이하의 값은 모두 0으로 처리한다.


        '''

        years = 5
        min_periods = 255  # 최소한의 연산 기간 1년

        df_stock = self.dict_multiple_cmp_cd[cmp_cd]
        df_stock = df_stock[df_stock["item_cd"] == item_cd].set_index('date')

        df_stock_tmp = df_stock[df_stock["multiple"] > 0]
        df_stock_tmp = df_stock_tmp[df_stock_tmp["multiple"] <= (
                    (df_stock_tmp["multiple"].rolling(window=(255 * years), min_periods=1).median()) * 3)]

        #     df_stock_tmp["history_multiple"] = df_stock_tmp["multiple"].rolling(window=(255*5), min_periods=1).median()
        df_stock_tmp["history_multiple"] = df_stock_tmp["multiple"].rolling(window=(255 * years),
                                                                            min_periods=min_periods).quantile(value_q,
                                                                                                              interpolation='nearest')
        df_stock_tmp["upside"] = df_stock_tmp["history_multiple"] / df_stock_tmp["multiple"]

        df_stock = pd.merge(left=df_stock, right=df_stock_tmp[["history_multiple", "upside"]], left_index=True,
                            right_index=True, how="left")

        df_stock["history_multiple"] = df_stock["history_multiple"].ffill()
        df_stock["upside"] = df_stock["history_multiple"] / df_stock["multiple"]
        df_stock.loc[df_stock["upside"] <= 0, "upside"] = 0

        df_stock_price = self.dict_df_stock[cmp_cd].copy()
        df_stock_price = df_stock_price[~df_stock_price["MarketCap"].isna()]
        df_stock_price["MarketCap"] = (df_stock_price["MarketCap"] / 100000000).astype("int64")
        df_merge = pd.merge(left=df_stock, right=df_stock_price[["Close", "MarketCap"]], how="right", left_index=True,
                            right_index=True)

        return df_merge

    def get_df_res_bulk(self, list_cmp_cd):

        list_df_res = deque([])
        df_res_bulk = pd.DataFrame()
        df_res = pd.DataFrame()
        for cmp_cd in tqdm(list_cmp_cd):

            for item_cd in range(900001, 900009):

                for value_q in [0.25, 0.5, 0.75]:
                    df_stock = self.make_column(cmp_cd, item_cd, value_q)
                    df_stock = df_stock[(~df_stock["cmp_cd"].isna())][["cmp_cd", "item_cd", "history_multiple", "upside"]]
                    df_stock = df_stock.reset_index()
                    df_stock = df_stock.rename(columns={"Date": "date"})
                    df_stock["value_q"] = value_q

                    list_df_res.append(df_stock)

        df_res_bulk = pd.concat(list_df_res)
        df_res_bulk["history_multiple"] = df_res_bulk["history_multiple"].fillna(0)
        df_res_bulk["upside"] = df_res_bulk["upside"].fillna(0)

        df_res_bulk.loc[df_res_bulk["upside"] == np.inf, "upside"] = 0

        df_res_bulk["item_cd"] = df_res_bulk["item_cd"].astype("int32")
        df_res_bulk["history_multiple"] = df_res_bulk["history_multiple"].astype("float32")
        df_res_bulk["upside"] = df_res_bulk["upside"].astype("float32")
        df_res_bulk["value_q"] = df_res_bulk["value_q"].astype("float16")

        df_res_bulk = df_res_bulk.reset_index(drop=True)

        return df_res_bulk

    def run(self):

        list_thread = []
        #  전체 종목 100개 단위 분할, 약 23개 스레드
        list_cmp_cd_t = list(self.dict_multiple_cmp_cd.keys())
        n = math.ceil(len(list_cmp_cd_t) / 4)
        list_cmp_cd_t = [list_cmp_cd_t[i * n:(i + 1) * n] for i in range((len(list_cmp_cd_t) + n - 1) // n)]

        pool = concurrent.futures.ProcessPoolExecutor(max_workers=4)

        start = time.time()

        for i, list_cmp_cd in enumerate(list_cmp_cd_t):
            list_thread.append(pool.submit(self.get_df_res_bulk, list_cmp_cd))

        df_res_bulk = pd.DataFrame()
        for t in concurrent.futures.as_completed(list_thread):
            df_res_bulk = pd.concat([df_res_bulk, t.result()])

        end = time.time()
        print(end - start)

        # 기존 데이터 백업
        file_name = 'df_multiple_history_' + datetime.datetime.today().strftime("%Y%m%d")

        with open(r'D:\MyProject\종목분석_환경\multiple_DB\df_multiple_history.pickle', 'rb') as fr:
            df_backup = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(df_backup, fw)
                del [[df_backup]]

        # 기존 데이터 백업
        file_name = 'dict_multiple_his_cmp_cd_' + datetime.datetime.today().strftime("%Y%m%d")

        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_his_cmp_cd.pickle', 'rb') as fr:
            df_backup = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(df_backup, fw)
                del [[df_backup]]

        # 기존 데이터 백업
        file_name = 'dict_multiple_his_date_' + datetime.datetime.today().strftime("%Y%m%d")

        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_his_date.pickle', 'rb') as fr:
            df_backup = pickle.load(fr)

            with open(r'D:\MyProject\종목분석_환경\multiple_DB\백업\{}.pickle'.format(file_name), 'wb') as fw:
                pickle.dump(df_backup, fw)
                del [[df_backup]]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\df_multiple_history.pickle', 'wb') as fw:
            pickle.dump(df_res_bulk, fw)

        # 1. key-value(cmp_cd)
        dict_multiple_his_cmp_cd = {}

        for cmp_cd in tqdm(df_res_bulk["cmp_cd"].unique()):
            dict_multiple_his_cmp_cd[cmp_cd] = df_res_bulk[df_res_bulk["cmp_cd"] == cmp_cd]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_his_cmp_cd.pickle', 'wb') as fw:
            pickle.dump(dict_multiple_his_cmp_cd, fw)
            del [dict_multiple_his_cmp_cd]

        # 2. key-value(date)
        dict_multiple_his_date = {}

        for v_date in tqdm(pd.to_datetime(df_res_bulk["date"].unique())):
            dict_multiple_his_date[v_date] = df_res_bulk[df_res_bulk["date"] == v_date]

        # save data
        with open(r'D:\MyProject\종목분석_환경\multiple_DB\dict_multiple_his_date.pickle', 'wb') as fw:
            pickle.dump(dict_multiple_his_date, fw)
            del [dict_multiple_his_date]

        print("[END]|" + datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S") + "|" + self.__class__.__name__)
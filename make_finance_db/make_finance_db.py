import pandas as pd
from tqdm import tqdm
import numpy as np
from config import API_KEY
import pymysql
import datetime
import pickle
from sqlalchemy import create_engine, Column, Integer, String, text


class make_finance_db():


    def __init__(self, term_typ):
        """
        :param term_typ: {q: 분기 , y: 연간, ttm: 4분기 합산}
        """
        self.term_typ = term_typ

        pymysql.install_as_MySQLdb()

        self.user_nm = API_KEY["MYSQL"]["ID"]
        self.user_pw = API_KEY["MYSQL"]["PW"]

        self.host_nm = API_KEY["MYSQL"]["HOST"]

        engine = create_engine("mysql+mysqldb://" + self.user_nm + ":" + self.user_pw + "@" + self.host_nm)

        conn = engine.connect()

        self.table_nm = "financial_statement_" + term_typ

        self.df_fin_master = pd.read_sql_query('SELECT * FROM financial_data.finance_master', conn)
        self.df_mysql      = pd.read_sql_query('SELECT * FROM financial_data.{}'.format(self.table_nm), conn)
        self.df_krx_info = pd.read_sql_query('SELECT * FROM financial_data.krx_stock_info', conn)

    # 손익계산서
    def get_df_income(self, cmp_cd, master_chk_val, dimension_val):

        f_type = "income"

        file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + dimension_val + ".csv"
        file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm

        try:
            df_csv = pd.read_csv(file_nm)
        except FileNotFoundError as e:
            print(e)
            df_csv = pd.DataFrame()
            return df_csv

        # 1. 데이터 구조 변경
        df_csv = df_csv.T
        df_csv.columns = df_csv.loc["손익계산서"]
        df_csv = df_csv.drop(index="손익계산서")

        list_col = list(df_csv.columns)

        # 빈값인 경우 리턴
        if len(df_csv) == 0:
            return df_csv

        # 2. 필요 칼럼 필터링
        list_item_nm = self.df_fin_master[self.df_fin_master["item_typ"] == "손익계산서"]["item_nm"].to_list()
        list_item_nm[0] = "매출액(수익)"

        # 우리 종금 CASE
        for index, value in enumerate(list_col):
            if value == "*(지배주주지분)연결당기순이익":
                list_col[index] = "당기순이익"


        df_csv.columns = list_col

        # 2-1. 개별 재무인 경우
        # 2023-04-12 지배지분 관련 데이터 수집 안함
        if master_chk_val == 1:
            list_item_nm.remove("지배지분 순이익")
            list_item_nm.remove("비지배지분 순이익")

        # 2-1. 금융업종인 경우
        if "매출액(수익)" not in df_csv.columns:
            list_item_nm.remove("매출액(수익)")
            list_item_nm.remove("매출원가")
            list_item_nm.remove("매출총이익")
            list_item_nm.remove("판매비와관리비")
            if master_chk_val != 1:
                list_item_nm.remove("지배지분 순이익")
                list_item_nm.remove("비지배지분 순이익")
            list_item_nm.remove("EBITDA")
            df_csv = df_csv[list_item_nm]

        else:
            df_csv = df_csv[list(set(list_item_nm) & set(list_col))
]
            df_csv = df_csv.rename(columns={"매출액(수익)": "매출액"})
            df_csv = df_csv[~(df_csv["매출액"] == "N/AN/A")]
            df_csv = df_csv.astype("float")

            df_csv = df_csv[~(df_csv["매출액"] <= 0)]
            # 투자지표로 분류돼있으나, 손익계산서 내에서 진행
            df_csv["GPM"] = (df_csv["매출총이익"] / df_csv["매출액"]) * 100
            df_csv["OPM"] = (df_csv["영업이익"] / df_csv["매출액"]) * 100
            df_csv["NPM"] = (df_csv["당기순이익"] / df_csv["매출액"]) * 100
            df_csv["매출원가율(%)"] = (df_csv["매출원가"] / df_csv["매출액"]) * 100

        df_csv = df_csv.reset_index().rename(columns={"index": "yymm"})

        # 3. 데이터 구조 변경 & item_cd 매핑
        df_csv = df_csv.melt(id_vars=["yymm"],
                             var_name="item_nm",
                             value_name="val")
        df_csv = pd.merge(left=df_csv, right=self.df_fin_master[["item_nm", "item_cd"]], on="item_nm")

        # 4. YYMM 형태 변경
        df_csv["yymm"] = df_csv["yymm"].str[:4] + df_csv["yymm"].str[-2:]
        df_csv["yymm"] = df_csv["yymm"].astype('int64')

        # 5. 칼럼 추가
        df_csv["fin_typ"] = master_chk_val
        df_csv["term_typ"] = dimension_val
        df_csv["cmp_cd"] = cmp_cd

        df_csv = df_csv[["fin_typ", "term_typ", "cmp_cd", "item_cd", "yymm", "val"]]

        # 6. 연간 데이터인 경우, 가장 최근값이 분기값으로 박혀있으면 제거
        if (dimension_val == "y") & (len(df_csv) > 1):

            if (df_csv.iloc[0]["yymm"] % 100 != df_csv.iloc[1]["yymm"] % 100):
                df_csv = df_csv.drop(index=0).reset_index(drop=True)

        return df_csv

    # 재무상태표
    def get_df_balancesheet(self, cmp_cd, master_chk_val, dimension_val):

        f_type = "balancesheet"

        if (dimension_val == "ttm"):
            file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + "q" + ".csv"
            file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm
        else:
            file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + dimension_val + ".csv"
            file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm

        try:
            df_csv = pd.read_csv(file_nm)
        except FileNotFoundError as e:
            print(e)
            df_csv = pd.DataFrame()
            return df_csv

        # 1. 데이터 구조 변경
        df_csv = df_csv.T
        df_csv.columns = df_csv.loc["재무상태표"]
        df_csv = df_csv.drop(index="재무상태표")

        # 빈값인 경우 리턴
        if len(df_csv) == 0:
            return df_csv

        # 2. 필요 칼럼 필터링
        list_item_nm = self.df_fin_master[self.df_fin_master["item_typ"] == "재무상태표"]["item_nm"].to_list()

        # 2-1. 금융업종인 경우
        if "자산총계" not in df_csv.columns:
            list_item_nm[0] = "자산"
            list_item_nm[1] = "자본"
            list_item_nm[2] = "부채"

            df_csv = df_csv[list_item_nm]
            df_csv = df_csv.rename(columns={"자산": "자산총계",
                                            "자본": "자본총계",
                                            "부채": "부채총계"})
        else:
            df_csv = df_csv[list_item_nm]

        df_csv = df_csv.reset_index().rename(columns={"index": "yymm"})

        # 3. 데이터 구조 변경 & item_cd 매핑
        df_csv = df_csv.melt(id_vars=["yymm"],
                             var_name="item_nm",
                             value_name="val")
        df_csv = pd.merge(left=df_csv, right=self.df_fin_master[["item_nm", "item_cd"]], on="item_nm")

        # 4. YYMM 형태 변경
        df_csv["yymm"] = df_csv["yymm"].str[:4] + df_csv["yymm"].str[-2:]
        df_csv["yymm"] = df_csv["yymm"].astype('int64')

        # 5. 칼럼 추가
        df_csv["fin_typ"] = master_chk_val
        df_csv["term_typ"] = dimension_val
        df_csv["cmp_cd"] = cmp_cd

        df_csv = df_csv[["fin_typ", "term_typ", "cmp_cd", "item_cd", "yymm", "val"]]

        # 6. 연간 데이터인 경우, 가장 최근값이 분기값으로 박혀있으면 제거
        if (dimension_val == "y") & (len(df_csv) > 1):
            if (df_csv.iloc[0]["yymm"] % 100 != df_csv.iloc[1]["yymm"] % 100) & (len(df_csv) > 1):
                df_csv = df_csv.drop(index=0).reset_index(drop=True)

        return df_csv

    # 투자지표

    def get_df_investment(self, cmp_cd, master_chk_val, dimension_val):

        f_type = "investment"

        file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + dimension_val + ".csv"
        file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm

        try:
            df_csv = pd.read_csv(file_nm)
        except FileNotFoundError as e:
            print(e)
            df_csv = pd.DataFrame()
            return df_csv

        if len(df_csv) == 0:
            return df_csv

        # 1. 데이터 구조 변경
        df_csv = df_csv.T
        df_csv.columns = df_csv.loc["투자지표"]
        df_csv = df_csv.drop(index="투자지표")

        # 빈값인 경우 리턴
        if len(df_csv) == 0:
            return 0

        # 2. 필요 칼럼 필터링
        # list_item_nm    = df_fin_master[df_fin_master["item_typ"] == "투자지표"]["item_nm"].to_list()

        # 2-1. 금융업종인 경우
        list_item_nm = []
        list_item_nm.append("주당순이익 EPS(원)")
        list_item_nm.append("주당순자산 BPS(원)")
        list_item_nm.append("주당배당금 DPS(원)")
        list_item_nm.append("주당매출액 SPS(원)")
        list_item_nm.append("주당현금흐름 CPS(원)")
        list_item_nm.append("부채비율(%)")
        list_item_nm.append("부채비율(%)")
        list_item_nm.append("유동비율(%)")
        list_item_nm.append("배당수익률(%)")
        list_item_nm.append("자기자본이익률 ROE(%)")
        list_item_nm.append("총자산이익률 ROA(%)")
        list_item_nm.append("투하자본이익률ROIC(%)")

        df_csv = df_csv[list_item_nm]
        df_csv = df_csv.rename(columns={"주당순이익 EPS(원)": "EPS",
                                        "주당순자산 BPS(원)": "BPS",
                                        "주당배당금 DPS(원)": "DPS",
                                        "주당매출액 SPS(원)": "SPS",
                                        "주당현금흐름 CPS(원)": "CFPS",
                                        "자기자본이익률 ROE(%)": "ROE",
                                        "총자산이익률 ROA(%)": "ROA",
                                        "투하자본이익률ROIC(%)": "ROIC",

                                        })

        df_csv = df_csv.reset_index().rename(columns={"index": "yymm"})

        # 3. 데이터 구조 변경 & item_cd 매핑
        df_csv = df_csv.melt(id_vars=["yymm"],
                             var_name="item_nm",
                             value_name="val")
        df_csv = pd.merge(left=df_csv, right=self.df_fin_master[["item_nm", "item_cd"]], on="item_nm")

        # 4. YYMM 형태 변경
        df_csv["yymm"] = df_csv["yymm"].str[:4] + df_csv["yymm"].str[-2:]
        df_csv["yymm"] = df_csv["yymm"].astype('int64')

        # 5. 칼럼 추가
        df_csv["fin_typ"] = master_chk_val
        df_csv["term_typ"] = dimension_val
        df_csv["cmp_cd"] = cmp_cd

        df_csv = df_csv[["fin_typ", "term_typ", "cmp_cd", "item_cd", "yymm", "val"]]

        # 6. 연간 데이터인 경우, 가장 최근값이 분기값으로 박혀있으면 제거
        if (dimension_val == "y") & (len(df_csv) > 1):
            if (df_csv.iloc[0]["yymm"] % 100 != df_csv.iloc[1]["yymm"] % 100) & (len(df_csv) > 1):
                df_csv = df_csv.drop(index=0).reset_index(drop=True)

        return df_csv

    # 증감값 및 증감률 칼럼 생성
    def make_col_change(self, df_finance, cmp_cd, master_chk_val, dimension_val):

        fin_typ = master_chk_val
        term_typ = dimension_val
        df_res = pd.DataFrame()
        if term_typ == "y":
            keys = [[1, "yoy"]]

        elif term_typ in ["q", "ttm"]:
            keys = [[1, "qoq"],
                    [4, "yoy"]]

        for key in keys:

            periods = key[0]
            freq = key[1]

            df_tmp = df_finance[(df_finance["fin_typ"] == fin_typ) & (df_finance["term_typ"] == term_typ)]
            df_tmp = df_tmp.sort_values(["cmp_cd", "item_cd", "yymm"])

            df_tmp["change_val"] = df_tmp["val"].diff(periods=periods)
            df_tmp["change_val"] = df_tmp["change_val"].fillna(0)

            a = np.array(df_tmp["change_val"], dtype=float)
            b = np.array(df_tmp["val"].shift(periods=periods).abs(), dtype=float)
            b = [0.00001 if x == 0 else x for x in b]  # 0값 대체

            df_tmp["change_pct"] = (a / b) * 100

            # 다른 item_cd와 증감 연산을 한 row 제거
            # 각 계정별 최초값의 변화율은 0으로 변환
            if periods == 1:
                df_tmp.loc[
                    df_tmp["yymm"] == df_tmp.groupby(["cmp_cd", "item_cd"])["yymm"].transform('min'), "change_val"] = 0
                df_tmp.loc[
                    df_tmp["yymm"] == df_tmp.groupby(["cmp_cd", "item_cd"])["yymm"].transform('min'), "change_pct"] = 0
            elif periods == 4:
                df_tmp.loc[df_tmp["yymm"] < (
                            df_tmp.groupby(["cmp_cd", "item_cd"])["yymm"].transform('min') + 100), "change_val"] = 0
                df_tmp.loc[df_tmp["yymm"] < (
                            df_tmp.groupby(["cmp_cd", "item_cd"])["yymm"].transform('min') + 100), "change_pct"] = 0

            # 증감이 0인값은 증감률도 0
            df_tmp.loc[df_tmp["change_val"] == 0, "change_pct"] = 0

            # 흑전 == 9999%로 정의
            df_tmp.loc[df_tmp["change_pct"] == np.inf, "change_pct"] = 9999

            df_tmp["freq"] = freq
            df_res = pd.concat([df_res, df_tmp])

        df_finance = df_res

        return df_finance

    # 신규 업데이트 데이터 생성
    def get_jemu_data(self):
        ## 2. 종목 정보
        dimension_val = self.term_typ

        # 1. 100개 단위로 list에 append
        list_res = []
        df_res = pd.DataFrame()

        count_size = 0
        for cmp_cd in tqdm(self.df_krx_info["Symbol"]):

            # 100개 단위로 list_res에 적재 후 초기화
            if (count_size % 100 == 0) & (count_size != 0):
                list_res.append(df_res)
                df_res = pd.DataFrame()

            count_size += 1

            for master_chk_val in [1, 31]:

                df_finance = pd.DataFrame()

                df_income = self.get_df_income(cmp_cd, master_chk_val, dimension_val)
                if len(df_income) == 0:
                    print(cmp_cd, master_chk_val, dimension_val)
                    continue
                df_balancesheet = self.get_df_balancesheet(cmp_cd, master_chk_val, dimension_val)
                if len(df_balancesheet) == 0:
                    continue
                df_investment = self.get_df_investment(cmp_cd, master_chk_val, dimension_val)
                if len(df_investment) == 0:
                    continue

                df_finance = pd.concat([df_income, df_balancesheet, df_investment])
                df_finance.loc[df_finance["val"].isna(), "val"] = 0
                df_finance["val"] = df_finance["val"].astype("float")

                # 증감값 및 증감률 칼럼 생성
                df_finance = self.make_col_change(df_finance, cmp_cd, master_chk_val, dimension_val)

                df_res = pd.concat([df_res, df_finance])

        list_res.append(df_res)

        # 2. 최종 데이터 concat
        df_res = pd.DataFrame()
        for num in tqdm(range(0, len(list_res))):
            df_res = pd.concat([df_res, list_res[num]])

        return df_res

    # 데이터베이스 생성
    def create_db(self):

        df_res = self.get_jemu_data()

        # 기존 데이터 백업
        today = str(datetime.date.today())
        # 백업 save data
        with open(r'D:\MyProject\밸류라인_크롤링\백업\{}_'.format(self.table_nm) + today + '.pickle', 'wb') as fw:
            pickle.dump(self.df_mysql, fw)

        # 1. 데이터 concat
        self.df_mysql = pd.concat([self.df_mysql[self.df_mysql["yymm"] <= 201312], df_res[df_res["yymm"] > 201312]])
        del df_res

        # 2. 데이터 drop dupicate
        self.df_mysql = self.df_mysql.drop_duplicates(["fin_typ", "term_typ", "cmp_cd", "item_cd", "yymm", "freq"]).reset_index(
            drop=True)

        self.df_mysql.loc[self.df_mysql["change_pct"] == -np.inf, "change_pct"] = -9999

        engine = create_engine("mysql+mysqldb://" + self.user_nm + ":" + self.user_pw + "@" + self.host_nm)
        conn = engine.connect()

        sql = text('DROP TABLE IF EXISTS financial_data.{};'.format(self.table_nm))
        engine.execute(sql)

        sub_size = 100000

        for length in tqdm(range(0, len(self.df_mysql), sub_size)):
            self.df_mysql.loc[length:length + sub_size - 1].to_sql(self.table_nm, engine, if_exists='append',
                                                              index=False, schema='financial_data')





from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
import pyperclip
import pymysql
from selenium.webdriver.common.keys import Keys
import pandas as pd
from tqdm import tqdm
from bs4 import BeautifulSoup
import requests
from sqlalchemy import create_engine
import time
from config import API_KEY
import multiprocessing
import random

# 크롬 드라이버 버젼이 바뀌면 Header의 크롬 드라이버 버젼도 바껴야한다!!!! (2023.06.07)
class CrawlingFinanceData():

    def __init__(self):

        self.list_err_cd = []

        # 세션 상태
        self.s = requests.Session()

    def make_csv(self, f_type, cmp_cd):

        request_url = "https://www.valueline.co.kr/finance/{}/{}".format(f_type, cmp_cd)
        headers = \
            {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
                , "Accept-Encoding": "gzip, deflate, br"
                , "Cache-Control": "max-age=0"
                , "Connection": "keep-alive"
                , "Content-Length": "105"
                , "Content-Type": "application/x-www-form-urlencoded"
                , "Host": "www.valueline.co.kr"
                , "Origin": "https://www.valueline.co.kr"
                , "Referer": request_url
                ,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
            }
        dimension = {
            "mrt": "ttm",
            "mry": "y",
            "mrq": "q"
        }

        master_chk = {
            "C": 31,
            "P": 1
        }

        for dimension_key in dimension:

            for master_chk_key in master_chk:

                dimension_val = dimension[dimension_key]
                master_chk_val = master_chk[master_chk_key]

                if (f_type == "balancesheet") & (dimension_val == "ttm"):
                    continue

                payloads = {
                    'dimension': dimension_key,
                    'bic_type': '',
                    'master_chk': master_chk_key,
                    'accounts_chk': 'I',
                    'view_chk': 'D',
                }

                r = self.s.post(request_url, data=payloads, headers=headers)
                soup = BeautifulSoup(r.text, 'html.parser')
                tables = soup.select('table')
                table = tables[0]
                table_html = str(table)

                # 정수&실수 데이터 중복되므로 정수 데이터 제거
                list_ess_data = table.find_all('span', {"class": "ess"})
                for x in list_ess_data:
                    table_html = table_html.replace(str(x), '')

                df = pd.read_html(table_html)[0]

                file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + dimension_val + ".csv"
                file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm

                df.to_csv(file_nm, index=False)

    def make_cmp_excel_data(self, list_cmp_cd, list_f_type):

        for cmp_cd in tqdm(list_cmp_cd):
            time.sleep(random.uniform(0.5, 2))
            for f_type in list_f_type:
                time.sleep(random.uniform(0.5, 2))
                while True:

                    count_broken = 0
                    try:
                        self.make_csv(f_type, cmp_cd)
                        break

                    except Exception as e:

                        if (str(e).find("Connection broken") > 0) or (str(e).find("Connection aborted") > 0):
                            # Connection broken 에러가 아닌 케이스 -> 재시도
                            print(cmp_cd + " " + str(e))
                            if count_broken < 10:
                                time.sleep(random.uniform(0.5, 2))
                                count_broken += 1
                            else:
                                continue

                        else:
                            # Connection broken 에러가 아닌 케이스일 경우 에러 리스트로 저장
                            print(cmp_cd + " " + str(e))
                            self.list_err_cd.append(cmp_cd)
                            break

    def run(self):

        driver = webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()))

        user_id = API_KEY["NAVER"]["ID"]
        user_pw = API_KEY["NAVER"]["PW"]

        # 1. 네이버 이동
        driver.get('http://naver.com')

        # 2. 로그인 버튼 클릭
        # elem = driver.find_element_by_class_name('link_login')
        elem = driver.find_element(By.CLASS_NAME, 'MyView-module__link_login___HpHMW')
        elem.click()

        # 3. ID 복사 붙여넣기
        elem_id = driver.find_element(By.ID,'id')
        elem_id.click()
        pyperclip.copy(user_id)
        elem_id.send_keys(Keys.CONTROL, 'v')
        time.sleep(1)

        # 4. PW 복사 붙여넣기
        elem_id = driver.find_element(By.ID,'pw')
        elem_id.click()
        pyperclip.copy(user_pw)
        elem_id.send_keys(Keys.CONTROL, 'v')
        time.sleep(1)

        # 5. 로그인 버튼 클릭
        driver.find_element(By.ID,'log.login').click()

        # 6. 밸류라인 이동 & 로그인 상태 세션
        driver.get('https://value.choicestock.co.kr/member/login')
        try:
            driver.find_elements(By.XPATH, '//*[@id="container"]/div/div[2]/ul/li[1]/a')[0].click()
        except:
            driver.get('https://value.choicestock.co.kr/member/login')
            driver.find_elements(By.XPATH, '//*[@id="container"]/div/div[2]/ul/li[1]/a')[0].click()

        ## 2. 종목 정보
        pymysql.install_as_MySQLdb()
        user_nm = API_KEY["MYSQL"]["ID"]
        user_pw = API_KEY["MYSQL"]["PW"]

        host_nm = API_KEY["MYSQL"]["HOST"]
        engine = create_engine("mysql+mysqldb://" + user_nm + ":" + user_pw + "@" + host_nm)

        conn = engine.connect()
        df_krx_info = pd.read_sql_query('SELECT * FROM financial_data.krx_stock_info', conn)

        # Selenium 세션 상태 가져오기
        self.s = requests.Session()

        for cookie in driver.get_cookies():
            c = {cookie['name'] : cookie['value']}
            self.s.cookies.update(c)

        list_f_type = ["income", "balancesheet", "investment"]

        list_err_cd = []
        list_thread = []
        #  전체 종목 100개 단위 분할, 약 23개 스레드
        n = 200
        list_cmp_cd_t = df_krx_info["Symbol"].to_list()

        list_cmp_cd_t = [list_cmp_cd_t[i * n:(i + 1) * n] for i in range((len(list_cmp_cd_t) + n - 1) // n)]

        start = time.time()

        for list_cmp_cd in list_cmp_cd_t:
            t = multiprocessing.Process(target=self.make_cmp_excel_data, args=(list_cmp_cd, list_f_type))
            t.start()
            list_thread.append(t)

        for t in list_thread:
            t.join()

        end = time.time()
        print(end - start)


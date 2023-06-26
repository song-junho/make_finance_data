from selenium import webdriver
import pyperclip
import time
from selenium.webdriver.common.keys import Keys
import pandas as pd
from tqdm import tqdm
import numpy as np
import time
import requests
import FinanceDataReader as fdr
import time
import threading

# 크롬 드라이버 버젼이 바뀌면 Header의 크롬 드라이버 버젼도 바껴야한다!!!! (2023.06.07)
def crawling_finance_data():

    def make_csv(f_type, cmp_cd):

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
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
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

                r = s.post(request_url, data=payloads, headers=headers)
                df = pd.read_html(r.text)[0]

                file_nm = "\\" + f_type + "_" + cmp_cd + "_" + str(master_chk_val) + "_" + dimension_val + ".csv"
                file_nm = r"D:\MyProject\밸류라인_크롤링\엑셀_데이터" + file_nm

                df.to_csv(file_nm, index=False)

    def make_cmp_excel_data(list_cmp_cd):
        for cmp_cd in tqdm(list_cmp_cd):

            for f_type in list_f_type:

                while True:

                    count_broken = 0
                    try:
                        make_csv(f_type, cmp_cd)
                        break

                    except Exception as e:

                        if str(e).find("Connection broken") > 0:
                            # Connection broken 에러가 아닌 케이스 -> 재시도
                            print(cmp_cd + " " + str(e))
                            if count_broken < 10:
                                count_broken += 1
                            else:
                                continue

                        else:
                            # Connection broken 에러가 아닌 케이스일 경우 에러 리스트로 저장
                            print(cmp_cd + " " + str(e))
                            list_err_cd.append(cmp_cd)
                            break

    driver = webdriver.Chrome(r'C:\Users\송준호\Downloads\chromedriver_win32 (7)\chromedriver.exe')

    user_id = "junho10000se"
    user_pw = "ghwnsthd!0212"

    # 1. 네이버 이동
    driver.get('http://naver.com')

    # 2. 로그인 버튼 클릭
    # elem = driver.find_element_by_class_name('link_login')
    elem = driver.find_element_by_class_name('MyView-module__link_login___HpHMW')
    elem.click()

    # 3. ID 복사 붙여넣기
    elem_id = driver.find_element_by_id('id')
    elem_id.click()
    pyperclip.copy(user_id)
    elem_id.send_keys(Keys.CONTROL, 'v')
    time.sleep(1)

    # 4. PW 복사 붙여넣기
    elem_id = driver.find_element_by_id('pw')
    elem_id.click()
    pyperclip.copy(user_pw)
    elem_id.send_keys(Keys.CONTROL, 'v')
    time.sleep(1)

    # 5. 로그인 버튼 클릭
    driver.find_element_by_id('log.login').click()

    # 6. 밸류라인 이동 & 로그인 상태 세션
    driver.get('https://value.choicestock.co.kr/member/login')
    driver.find_elements_by_xpath('//*[@id="container"]/div/div[2]/ul/li[1]/a')[0].click()

    ## 2. 종목 정보
    df_krx_info = fdr.StockListing("KRX")

    df_krx_info = df_krx_info[df_krx_info["Market"].isin(["KOSPI", "KOSDAQ", "KOSDAQ GLOBAL"])]
    # df_krx_info = df_krx_info[~df_krx_info["ListingDate"].isna()]
    df_krx_info = df_krx_info[~df_krx_info["Name"].str.contains("스팩")]
    df_krx_info = df_krx_info.sort_values("Code").reset_index(drop=True)
    df_krx_info = df_krx_info.rename(columns={"Code": "Symbol"})
    df_krx_info = df_krx_info[~(df_krx_info["Symbol"].str[-1] != "0")].reset_index(drop=True)

    # Selenium 세션 상태 가져오기
    s = requests.Session()

    for cookie in driver.get_cookies():
        c = {cookie['name'] : cookie['value']}
        s.cookies.update(c)


    list_f_type = ["income", "balancesheet", "investment"]

    list_err_cd = []
    list_thread = []
    #  전체 종목 100개 단위 분할, 약 23개 스레드
    n = 100
    list_cmp_cd_t = df_krx_info["Symbol"].to_list()
    list_cmp_cd_t = [list_cmp_cd_t[i * n:(i + 1) * n] for i in range((len(list_cmp_cd_t) + n - 1) // n)]

    start = time.time()

    for list_cmp_cd in (list_cmp_cd_t):
        t = threading.Thread(target=make_cmp_excel_data, args=(list_cmp_cd,))
        t.start()
        list_thread.append(t)

    for t in list_thread:
        t.join()

    end = time.time()
    print(end - start)

import pandas_datareader.data as web
import datetime
import pymysql
import pandas as pd

# data 초기화
conn = pymysql.connect(host='localhost', user='root', password='sd',
                       db='ysj', charset='utf8')
curs = conn.cursor()
curs.execute("delete from stock")

# 코스피 전체종목조회
kospi_stocks = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt', header=0)[0]
kospi_stocks.종목코드 = kospi_stocks.종목코드.map('{:06d}'.format) + ".ks"

# 코스닥 전체종목조회
kosdaq_stocks = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt', header=0)[0]
kosdaq_stocks.종목코드 = kosdaq_stocks.종목코드.map('{:06d}'.format) + ".KQ"

# 코스피 전체종목 주가저장
for code in kospi_stocks.종목코드.head(2000):
    try:
        facebook = web.DataReader(code, "yahoo", datetime.datetime(2013, 1, 1), None)
        for i in range(int(facebook.size/6)):
            sql = "insert into stock(code,yyyymmdd,High,Low,Open,Close,Volume,Adj) values(%s, %s, %s, %s, %s, %s, %s, %s)"
            curs.execute(sql, (str(code),
                               str(facebook.iloc[i, 1:1]),
                               str(facebook.iloc[i, 0]),
                               str(facebook.iloc[i, 1]),
                               str(facebook.iloc[i, 2]),
                               str(facebook.iloc[i, 3]),
                               str(facebook.iloc[i, 4]),
                               str(facebook.iloc[i, 5])))
        conn.commit()
    except:
        print(code)
conn.close()

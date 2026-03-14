from zhongwen.時 import 自起日按日列舉迄今, 取日期
from zhongwen.庫 import 結果批次寫入
from zhongwen.表 import 可顯示
from diskcache import Cache
from pathlib import Path
from fhopecc import env
import logging

上市每日收盤行情庫 = Path.home() / '.twse_crawler' / '資料庫' / '上市每日收盤行情庫'

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
logger = logging.getLogger(Path(__file__).stem)

def 抓取上市個股日成交資訊():
    '證交所開放應用程式介面(OpenAPI)'
    from zhongwen.檔 import 抓取
    from zhongwen.時 import 今日
    from zhongwen.數 import 取數值
    import pandas as pd
    url = 'https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL'
    logger.info('抓取上市個股日成交資訊')
    json = 抓取(url, 回傳資料形態='json') 
    df = pd.DataFrame(json)
    df.columns = ['證券代號', '證券名稱', '成交股數', '成交金額'
                 ,'開盤價', '最高價', '最低價', '收盤價'
                 ,'漲跌(+/-)', '成交筆數']
    df['交易日期'] = 今日()
    for c in df.columns[2:10]:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    return df

@cache.memoize('抓取上市每日收盤行情', expire=8*60*60)
@結果批次寫入(上市每日收盤行情庫, '上市每日收盤行情表', '交易日期', 自起日按日列舉迄今(取日期('113.11.1')))
def 抓取上市每日收盤行情(日期):
    '''
    一、證交所每日下午1:40更新。
    二、欄位：證券代號、證券名稱、成交股數、成交筆數、成交金額、
              開盤價、最高價、最低價、收盤價、漲跌(+/-)、漲跌價差、
              最後揭示買價、最後揭示買量、最後揭示賣價、最後揭示賣量、
              本益比、交易日期。
    三、每次爬取後停2秒，避免被判定為爬蟲程式致禁止連線。
    '''
    from zhongwen.時 import 正式民國日期, 今日, 取日期
    from requests.exceptions import HTTPError
    from zhongwen.檔 import 抓取
    from zhongwen.數 import 取數值 
    import time
    import pandas as pd
    date_f = 取日期(日期).strftime('%Y%m%d') 
    logger.info(f'抓取上市{正式民國日期(日期)}收盤行情')

    url  =  'https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX'
    url += f'?date={date_f}&type=ALLBUT0999&response=json'
    try:
        j = 抓取(url, 回傳資料形態='json')
        for t in j['tables']:
            if '每日收盤行情' in t['title']:
                df = pd.DataFrame(t['data'], columns=t['fields'])
                break
    except KeyError:
        return pd.DataFrame()
    from zhongwen.表 import 表示
    df['交易日期'] = 日期
    cols = df.columns[2:9]

    df[cols] = (
        df[cols]
        .replace({'--': '0', ',': ''}, regex=True)
        .apply(pd.to_numeric)
    )

    cols = df.columns[11:16]
    df[cols] = (
        df[cols]
        .replace({'--': '0', ',': ''}, regex=True)
        .apply(pd.to_numeric)
    )
    time.sleep(2)
    return df

@可顯示
def 爬取個股每月日成交資訊(股票代號, 年數, 月數, 重載=False):
    from pathlib import Path
    db = Path(__file__).parent / '證交所資料'
    import sqlite3
    with sqlite3.connect(db) as c:
        import pandas as pd
        sql =  "select * from 個股日成交資訊 where "
        sql += f"股票代號='{股票代號}' and "
        from zhongwen.date import 月起迄
        ms, me = 月起迄(年數, 月數)
        sql += f"日期 between '{ms}' and '{me}'"
        try:
            if 重載: raise pd.errors.DatabaseError('使用者要求重新爬取資料')
            df = pd.read_sql_query(sql, c, index_col='index')
            if df.empty: raise pd.errors.DatabaseError('尚無該月資料')
            return df
        except pd.errors.DatabaseError: pass

        import requests as req 
        url = "https://www.twse.com.tw/pcversion/zh/exchangeReport/STOCK_DAY"
        query = {"respose":'json'
                ,"date":f'{年數:04}{月數:02}01'
                ,"stockNo":股票代號
                }
        r = req.post(url, data=query)
        j = r.json()
        import pandas as pd
        df = pd.DataFrame(j['data'], columns=j['fields'])
        df['股票代號'] = '006208'
        from zhongwen.date import 取日期
        df['日期'] = df.日期.map(取日期)
        df.to_sql('個股日成交資訊', c, if_exists='append')
        return df

@可顯示
@cache.memoize(expire=180*24*60*60)
def 證券國際證券辨識號碼一覽表():
    '中華民國證券市場編碼原則由臺灣證券交易所統一賦予' 
    import pandas as pd
    # urls = [f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={i}" for i in range(1, 12)]
    urls = [f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={i}" for i in [2, 4, 5]]
    def 抓取(url):
        df = pd.read_html(url, encoding="cp950")[0]
        df.columns = df.iloc[0]
        df = df.iloc[2:]
        return df
    df = pd.concat([抓取(url) for url in urls])
    df = df.reset_index(drop=True)

    def 展開代號及名稱(s):
        import re
        pat = r'(\w+)\W(.+)'
        if m:=re.match(pat, s):
            return [m[1], m[2]]
        return [s, '']
    df['有價證券代號及名稱'] = df.有價證券代號及名稱.map(展開代號及名稱)
    def 取代號(l):
        return l[0].strip()
    def 取名稱(l):
        return l[1].strip()
    df['代號'] = df.有價證券代號及名稱.map(取代號)
    df['名稱'] = df.有價證券代號及名稱.map(取名稱)
    return df

@cache.memoize('抓上市公司基本資料', expire=24*60*60)
def 抓上市公司基本資料():
    '主鍵公司代號'
    from zhongwen.檔 import 抓取
    import pandas as pd
    url = 'https://openapi.twse.com.tw/v1/opendata/t187ap03_L'
    json = 抓取(url, 回傳資料形態='json')
    return pd.DataFrame(json)

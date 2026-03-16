from zhongwen.batch_data import 結果批次寫入, 載入批次資料, 增加定期更新, 通知執行時間
from zhongwen.庫 import 結果批次寫入
from zhongwen.時 import 自指定月份迄上月
from zhongwen.date import 自起算民國年逐年列舉迄今年, 自起算民國年月逐月列舉迄上個月, 今日
from zhongwen.batch_data import 期日資料批次寫入
from diskcache import Cache
from pathlib import Path
from functools import partial, cache as fcache
import pandas as pd
import logging
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)


月營收彙總表資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '月營收彙總表.db'
月自結損益彙總庫 = Path.home() / '.twse_crawler' / '資料庫' / '月自結損益彙總庫'
自結損益資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '自結盈餘彙總表.db'
股利分派資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '股利分派資料庫'
股利資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '股利資料庫'

def 爬取公司基本資料():
    return 抓公司基本資料()

@cache.memoize('抓公司基本資料', expire=24*60*60)
def 抓公司基本資料(加入興櫃公司=False):
    '''
    一、預設僅包含上市櫃公司。
    二、排除存託憑證。
    三、主鍵為公司代號。
    四、尚未實作加入興櫃公司功能。
    '''
    from zhongwen.數 import 取數值
    from zhongwen.文 import 刪空格
    from zhongwen.時 import 取日期
    from io import StringIO
    import requests
    import pandas as pd
    if 加入興櫃公司:
        raise NotImplementedError(f'抓公司基本資料尚未實作加入興櫃公司功能！')
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t51sb01"
    payload = {'encodeURIComponent': '1'
              ,'step': '1'
              ,'firstin':'1'
              ,'TYPEK':'sii'
              }
    def 爬取資料(類型):
        payload['TYPEK'] = 類型
        response = requests.post(url, data=payload, verify=False)
        content = StringIO(response.text)
        return pd.read_html(content)[0]
    df = pd.concat(爬取資料(類型) for 類型 in ['sii', 'otc', 'rotc'] )
    df = df.query('公司名稱 != "公司名稱"') 
    df.rename(columns=刪空格, inplace=True)
    df['實收資本額(元)'] = df['實收資本額(元)'].astype('int64')
    df['普通股每股面額'] = df.普通股每股面額.str.extract(r'新台幣 (\d+.\d+)元').astype('float')
    df['股數'] = df['已發行普通股數或TDR原發行股數']
    df['股數'] = df.股數.map(取數值)
    df['成立日期'] = df.成立日期.map(取日期)
    df.loc[:,'上市櫃日期'] = df.上市日期.fillna(df.上櫃日期).map(取日期)
    df = df.query("not 產業類別=='存託憑證'")
    df = df.query("上市櫃日期.notna()")
    return df

@通知執行時間
@結果批次寫入(月營收彙總表資料庫, '月營收彙總表', '營收月份', 自指定月份迄上月('10201'))
def 抓取月營收彙總表(營收月份):
    '資料月份未指定則自102年1月抓取至上月'
    from zhongwen.時 import 取日期, 取期間, 取民國年月
    from zhongwen.檔 import 抓取
    import pandas as pd
    logger.info(f'抓取{取民國年月(營收月份)}營收彙總表……')
    民國年數 = 營收月份.year - 1911
    月數 = 營收月份.month
    urls = [f"https://mopsov.twse.com.tw/nas/t21/{cls}/t21sc03_{民國年數}_{月數}.csv" 
            for cls in ['sii', 'otc']
           ]
    dtypes = {'公司代號':str
             ,'營業收入-當月營收':'int64'
             ,'營業收入-上月營收':'int64'
             ,'營業收入-去年當月營收':'int64'
             ,'營業收入-上月比較增減(%)':float
             ,'營業收入-去年同月增減(%)':float
             ,'累計營業收入-當月累計營收':'int64'
             ,'累計營業收入-去年累計營收':'int64'
             ,'累計營業收入-前期比較增減(%)':float
             }
    converters = {"出表日期":取日期
                 ,"資料年月":取期間
                 }

    df = pd.concat([pd.read_csv(抓取(url, 回傳資料形態='StringIO')
                               ,dtype=dtypes
                               ,converters=converters
                               ) 
                    for url in urls], ignore_index=True)

    if df.empty:
        # raise 網路查無資料(f'查無{取民國年月(營收月份)}營收彙總表！')
        logger.error(f'查無{取民國年月(營收月份)}營收彙總表！')

    df.rename(columns={'資料年月':'營收月份'}, inplace=True)
    df['營業收入-當月營收'] = df['營業收入-當月營收']*1000
    df['營業收入-上月營收'] = df['營業收入-上月營收']*1000
    df['營業收入-去年當月營收'] = df['營業收入-去年當月營收']*1000
    df['累計營業收入-當月累計營收'] = df['累計營業收入-當月累計營收']*1000
    df['累計營業收入-去年累計營收'] = df['累計營業收入-去年累計營收']*1000
    df['營業收入-上月比較增減(%)'] = df['營業收入-上月比較增減(%)'] / 100
    df['營業收入-去年同月增減(%)'] = df['營業收入-去年同月增減(%)'] / 100
    df['備註'] = df.備註.fillna('')
    logger.info(f'完成')
    return df

@fcache
@通知執行時間
@結果批次寫入(月自結損益彙總庫, '月自結損益彙總表', '自結損益月份', 自指定月份迄上月('10201'))
def 抓取月自結損益彙總表(自結損益月份=None, 重抓次數=0):
    '主鍵為公司代號'
    from zhongwen.時 import 取期間, 取民國月份, 取民國年月
    from zhongwen.檔 import 抓取
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    import time
    m = 取期間(自結損益月份)
    logger.info(f'抓取{取民國月份(m)}自結損益彙總表……')
    url = 'https://mopsov.twse.com.tw/mops/web/ajax_t138sb01'
    # CK2=1 為按月自結
    query = {'step':1, 'firstin':1, 'CK1':1, 'CK2':1, 'YM1':取民國年月(m)} 

    df1 = pd.DataFrame()
    for CK1 in [1, 2, 3]: # 上市、上櫃、興櫃
        query['CK1'] = CK1
        c = 抓取(url, 抓取方式='post', 參數=query, 回傳資料形態='StringIO')
        try:
            _df1 = pd.read_html(c, converters={'公司代號':str})
        except ValueError:
            _df1 = pd.DataFrame()
        df1 = pd.concat([df1, *_df1])
    try:
        df1.dropna(subset='公司代號', inplace=True)
    except KeyError:
        time.sleep(50) # 每抓取網頁 8 次會停止回應 50 秒，2024-12-19修正
        if 重抓次數 < 2:
            logger.error(f'第{重抓次數+1}次重抓{取民國年月(m)}自結損益資料！')
            return 抓取月自結損益彙總表(自結損益月份, 重抓次數=重抓次數+1)
        else:
            logger.error(f'抓取{取民國年月(m)}自結損益資料失敗！')
            return pd.DataFrame()

    df1 = df1.query('公司代號!="公司代號"')
    數值欄位 = ['本月營業損益'
               ,'本月稅前損益'
               ,'本年累計營業損益'
               ,'本年累計稅前損益'
               ,'本月合併營業損益'
               ,'本月合併稅前損益'
               ,'本年累計合併營業損益'
               ,'本年累計合併稅前損益'
               ]
    for c in 數值欄位:
        try:
            df1[c] = pd.to_numeric(df1[c], errors='coerce')*1000
        except KeyError as e:
            logger.error(f'無【{e.args[0]}】欄位')
    df1['自結損益月份'] = m
    del df1[0]
    del df1[1]
    return df1

@通知執行時間
@結果批次寫入(股利資料庫, '股利表', '公告民國年度', range(105, 今日().year-1911+1))
def 抓取公司股利分派公告資料彙總表(公告民國年度):
    '批號為公告民國年度'
    from zhongwen.時 import 取日期, 全是日期嗎, 取期間
    from zhongwen.檔 import 抓取
    from zhongwen.表 import 顯示
    from twse_crawler.股利分派情形爬蟲 import 取股利所屬年度
    from io import StringIO
    import pandas as pd
    import requests

    if 全是日期嗎(公告民國年度):
        return 抓取公司股利分派公告資料彙總表(公告民國年度.year-1911)
    logger.info(f'抓取{公告民國年度}年度公司股利分派公告資料彙總表……')
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t108sb27"
    資料={"encodeURIComponent":1
         ,"step":1
         ,"firstin":1
         ,"off":1
         ,"year":公告民國年度
         }
    def 爬取資料(類型):
        資料['TYPEK'] = 類型
        try:
            html = 抓取(url, 抓取方式='post', 資料=資料)
            df = pd.concat(pd.read_html(StringIO(html)))
            # if 類型=='otc':
                # 顯示(html)
                # 顯示(df, 顯示筆數=2000)
            return df
        except ValueError:
            logger.info(f"屬{類型}之公司尚未公告{公告民國年度}年度股利資料！")
            return pd.DataFrame()
        except requests.exceptions.ConnectionError:
            logger.error(f"連接{url}被證交所中斷！")
            return pd.DataFrame()

    # 僅抓取上市及上櫃，不抓取興櫃rotc及公發pub公司
    df = pd.concat(爬取資料(類型) for 類型 in ['sii', 'otc'])
    if df.empty:
        return df
    df.columns = [''.join(c for c in set(cols) if c not in ['現金股利', '股票股利'])
                  for cols in df.columns.values]
    renamer = {'盈餘分配之現金股利(元/股)':'盈餘配息'
              ,'盈餘分配之股東現金股利(元/股)':'盈餘配息'
              ,'法定盈餘公積、資本公積發放之現金(元/股)':'公積配息'
              ,'盈餘轉增資配股(元/股)':'盈餘配股'
              ,'法定盈餘公積、資本公積轉增資配股(元/股)':'公積配股'
              ,'法定盈餘公積發放之現金(元/股)':'法定公積配息'
              ,'法定盈餘公積轉增資配股(元/股)':'法定公積配股'
              ,'資本公積發放之現金(元/股)':'資本公積配息'
              ,'資本公積轉增資配股(元/股)':'資本公積配股'
              ,'特別股配發現金股利(元/股)':'特別股配息'
              }
    df = df.rename(columns=renamer)

    df = df.query('公司代號!="公司代號"')
    股利項目 = ['盈餘配股', '公積配股','盈餘配息', '公積配息'
               ,'法定公積配息', '法定公積配股'
               ,'資本公積配息', '資本公積配股', '特別股配息'
               ]
    當年度實際股利項目 = [c for c in df.columns if c in 股利項目]
    df = df.dropna(subset=當年度實際股利項目, how='all')
    try:
        df.loc[:, '股利所屬年度'] = df.股利所屬期間.map(取股利所屬年度)
        logger.info(df.query('股利所屬年度.isna()'))
        df.loc[:, '股利所屬年度'] = df.股利所屬年度.fillna(公告民國年度-1)
    except AttributeError:
        df.loc[:, '股利所屬年度'] = df.股利所屬年度.str.extract(r'(^\d+)'
                                      ,expand=False).astype('int8')
    for c in df.columns:
        if c[-1] == '日' or c == '公告日期':
            df[c] = df[c].astype(object) # 先將類型轉為通用物件
            df.loc[:, c] = df[c].map(取日期)
    df['公告民國年度'] = 公告民國年度
    df.loc[:, '公告民國年度'] = df.公告民國年度.astype('int8')
    df.loc[:, '股利所屬年度'] = df.股利所屬年度.astype('int8')
    return df

class 網路查無資料(Exception): pass

@期日資料批次寫入(月營收彙總表資料庫, '月營收彙總表', '營收資料時期', 自起算民國年月逐月列舉迄上個月(90, 6))
def 抓取網頁式月營收彙總表(資料月份=None):
    "90年6月至101年12月營收彙總僅 html 格式"
    pass

def 抓取庫藏股統計彙總表():
    '爬取近一年庫藏股統計彙總表'
    from zhongwen.file import chrome
    from selenium.webdriver.common.by import By
    import time
    c = chrome()
    url = 'https://mopsov.twse.com.tw/mops/web/t35sc09'
    c.get(url)

    from selenium.webdriver.support.ui import Select
    xpath = '/html/body/center/table/tbody/tr/td/div[4]/table/tbody/tr/td/div/table/tbody/tr/td[3]/div/div[3]/form/table/tbody/tr/td[2]/table/tbody/tr[1]/td[3]/div/div/select'
    t = c.find_element(By.XPATH, xpath)
    市場別 = Select(c.find_element(By.XPATH, xpath))
    市場別.select_by_visible_text("上市")

    xpath = '//*[@id="RD"]'
    排序 = Select(c.find_element(By.XPATH, xpath))
    排序.select_by_visible_text("以公司代號排列")

    xpath = '//*[@id="d1"]'
    起日 =  c.find_element(By.XPATH, xpath)
    from zhongwen.date import 今日, 民國日期
    起日_ = 今日().replace(year=今日().year-1)
    起日.send_keys(民國日期(起日_))

    xpath = '//*[@id="d2"]'
    迄日 =  c.find_element(By.XPATH, xpath)
    迄日.send_keys(民國日期(今日()))
    
    xpath = '/html/body/center/table/tbody/tr/td/div[4]/table/tbody/tr/td/div/table/tbody/tr/td[3]/div/div[3]/form/table/tbody/tr/td[4]/table/tbody/tr/td[2]/div/div/input'
    查詢鈕 = c.find_element(By.XPATH, xpath)
    查詢鈕.click()
    import time
    time.sleep(1)
    import pandas as pd
    dfs = pd.read_html(c.page_source)
    df = dfs[3]
    df.columns = df.iloc[14]
    df = df.iloc[16:]
    return df


def 抓法說會一覽表頁面():
    from zhongwen.file import 抓取
    '''
fetch("https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1", {
  "headers": {
    "accept": "*/*",
    "accept-language": "zh-TW,zh;q=0.9,en;q=0.8,zh-CN;q=0.7",
    "content-type": "application/x-www-form-urlencoded",
    "sec-ch-ua": "\"Not.A/Brand\";v=\"8\", \"Chromium\";v=\"114\", \"Google Chrome\";v=\"114\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin"
  },
  "referrerPolicy": "no-referrer",
  "body": "encodeURIComponent=1&step=1&firstin=1&off=1&TYPEK=sii&year=112&month=&co_id=",
  "method": "POST",
  "mode": "cors",
  "credentials": "include"
});
'''
    url = "https://mopsov.twse.com.tw/mops/web/ajax_t100sb02_1"
    return 抓取(url, 抓取方式='post', 參數={'TYPEK':'sii', 'year':'112'})

def 抓取普通股股利分派頻率彙總表():
    url = 'https://mopsov.twse.com.tw/mops/web/ajax_t05sb12'
    from zhongwen.file import 抓取
    資料={"step":1
         ,"TYPEK":"sii"
         ,"DIV_TIME":1,"DIV_TIME":2,"DIV_TIME":3} 
    p = 抓取(url, 抓取方式='post', 資料=資料)
    資料['TYPEK']="otc"
    p2 = 抓取(url, 抓取方式='post', 資料=資料)
    import pandas as pd
    df = pd.concat([df for df in pd.read_html(p) + pd.read_html(p2) if isinstance(df, pd.DataFrame)])
    排除條件='not 公司代號=="公司代號"'
    df = df.query(排除條件)
    from zhongwen.text import 刪空格
    df.rename(columns=刪空格, inplace=True)
    df.set_index('公司代號', inplace=True)
    return df

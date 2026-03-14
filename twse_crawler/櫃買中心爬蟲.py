from zhongwen.時 import 取日期, 自起日按日列舉迄今
from zhongwen.庫 import 結果批次寫入
from diskcache import Cache
from pathlib import Path
from fhopecc import env
import logging

上櫃股票行情庫 = Path.home() / '.twse_crawler' / '資料庫' / '上櫃股票行情庫'
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
logger = logging.getLogger(Path(__file__).stem)

@結果批次寫入(上櫃股票行情庫, '上櫃股票行情表', '交易日期', 自起日按日列舉迄今('113.11.1'))
def 抓取上櫃股票行情(日期=None):
    """
    一、欄位：代號、名稱、收盤、漲跌、開盤、最高、最低、均價、
              成交股數、成交金額(元)、成交筆數、
              最後買價、最後買量(千股)、最後賣價、最後賣量(千股)、
              發行股數、次日參考價、次日漲停價、次日跌停價
    """
    from zhongwen.時 import 取正式民國日期, 取民國日期, 今日 
    from zhongwen.數 import 取數值 
    from zhongwen.文 import 刪空格
    from zhongwen.檔 import 抓取
    import datetime 
    import pandas as pd
    if not 日期:
        日期=今日()
    p_date = 取民國日期(日期, 格式='%Y/%m/%d') 
    url = f'https://www.tpex.org.tw/www/zh-tw/afterTrading/dailyQuotes'
    logger.info(f'抓取上櫃{取正式民國日期(日期)}收盤行情')
    logger.debug(f'url:{url}')
    參數 = {'date':p_date, 'response':'json'}
    json = 抓取(url, 抓取方式='post', 參數=參數, return_json=True)
    df = pd.DataFrame(json['tables'][0]['data'], columns=json['tables'][0]['fields'])
    df.columns = map(刪空格, df.columns)
    df['交易日期'] = 日期

    if pd.__version__.startswith('2.'):
        df.iloc[:, 2:19] = df.iloc[:, 2:19].map(取數值)
    else:
        # df.iloc[:, 2:19] = df.iloc[:, 2:19].applymap(取數值)
        cols = df.columns[2:19]

        df[cols] = df[cols].map(取數值)
        #(
        #     df[cols]
        #     .replace({'--': '0', ',': '', ' 0-':'0'}, regex=True)
        #     .apply(pd.to_numeric)
        # )
    return df

def 取開放應用程式介面中文欄名(欄位英文名, 開放應用程式介面定義檔超連結, 資料表代碼):
    from zhongwen.檔 import 抓取
    json = 抓取(開放應用程式介面定義檔超連結, 回傳資料形態='json')
    資料表定義 = json['components']['schemas'][資料表代碼]#['bond_ISSBD5_data']
    return [資料表定義['properties'][c]['description'] for c in 欄位英文名]

@cache.memoize('抓轉交換債發行資料', expire=24*60*60)
def 抓轉交換債發行資料():
    '主鍵是【債券代碼】，另【機構代碼】是發行者代碼，即是公司代碼'
    from zhongwen.檔 import 抓取
    import pandas as pd

    url = 'https://www.tpex.org.tw/openapi/v1/bond_ISSBD5_data'
    json = 抓取(url, 回傳資料形態='json')   
    df = pd.DataFrame(json)

    url = 'https://www.tpex.org.tw/openapi/swagger.json'
    df.columns = 取開放應用程式介面中文欄名(df.columns, url, 'bond_ISSBD5_data')

    return df

def 抓上櫃股票基本資料():
    '主鍵公司代號'
    from zhongwen.檔 import 抓取
    import pandas as pd
    url = 'https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O'
    json = 抓取(url, 回傳資料形態='json')
    print(type(json[0]['TitleOfSpokesman']))
    # print(pd.DataFrame(json))
    return pd.DataFrame(json)

from zhongwen.batch_data import 結果批次寫入, 通知執行時間, 增加定期更新
from diskcache import Cache, Index
from pathlib import Path
import functools
import logging

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / '.twse_crawler' / '快取' / Path(__file__).stem)

重大訊息庫 = Path.home() / '.twse_crawler' / '資料庫' / '重大訊息庫'

財務報告數值快取檔路徑 = Path.home() / '.twse_crawler' / '快取' / '公告財務報告快取檔'
財務報告數值快取檔路徑 = str(財務報告數值快取檔路徑)
財務報告數值快取檔 = Index(財務報告數值快取檔路徑)

股利公告快取路徑 = Path.home() / '.twse_crawler' / '快取' / '股利公告快取'
股利公告快取路徑 = str(股利公告快取路徑)
股利公告快取 = Index(股利公告快取路徑)

@通知執行時間
@結果批次寫入(重大訊息庫, '重大訊息表', '歸屬日期')
def 爬取重大訊息(歸屬日期):
    import twse_crawler.重大訊息爬蟲
    import json
    df = twse_crawler.重大訊息爬蟲.抓取重大訊息(歸屬日期)
    df['詳細資料'] = df.詳細資料.map(json.dumps)
    return df

@通知執行時間
@cache.memoize('爬取近一週重大訊息', expire=12*60*60)
def 爬取近一週重大訊息():
    from zhongwen.時 import 一週前, 自起日按日列舉迄今
    import requests
    try:
        for 歸屬日期 in 自起日按日列舉迄今(一週前):
            爬取重大訊息(歸屬日期)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(f"發生錯誤：{e}")

def 爬取近一季重大訊息():
    from zhongwen.時 import 一季前, 自起日按日列舉迄今
    import requests
    try:
        for 歸屬日期 in 自起日按日列舉迄今(一季前):
            爬取重大訊息(歸屬日期)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(f"發生錯誤：{e}")

@functools.cache
@通知執行時間
@cache.memoize('載入近一季重大訊息', expire=8*60*60)
def 載入近一季重大訊息(股票=None):
    '''
    一、主鍵：公司代號、發言日期、發言時間。
    二、欄位：公司名稱、主旨、歸屬日期。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.庫 import 轉儲存字串
    from zhongwen.時 import 一季前 
    import sqlite3
    import pandas as pd

    if 股票:
        股票代號 = 查股票代號(股票)
        股票簡稱 = 查股票簡稱(股票)
        df = 載入近一季重大訊息()
        df = df.query('公司代號==@股票代號')
        if df.empty:
            import warnings
            warnings.warn(f'{股票簡稱}無近一季重大訊息') 
        return df

    爬取近一週重大訊息()

    日期欄位=['發言日期', '歸屬日期']
    with sqlite3.connect(重大訊息庫) as c:
        一季前字串 = 轉儲存字串(一季前)
        query = f'select * from 重大訊息表 where 發言日期 >= "{一季前字串}"'
        df = pd.read_sql_query(query, c, parse_dates=日期欄位)
        df = df.query('發言日期 >= @一季前')
        del df['index']
        return df

def 取本季重大訊息歸戶表():
    '''
    一、主鍵：公司代號。
    二、欄位：本季重大訊息。
    '''
    from zhongwen.時 import 取民國日期
    df = 載入近一季重大訊息()
    s = df.groupby('公司代號').apply(
            lambda rs: "；".join([f'{取民國日期(r.歸屬日期)}：{r.主旨}' 
                   for r in rs.sort_values('歸屬日期', ascending=False).itertuples()]))
    s.name = '本季重大訊息'
    return s.to_frame().reset_index()

公告財務報告表欄位 = ['公司代號', '公司名稱', '財報日期', '財報類型', '發言日期'
                     ,'營收', '營利', '稅前淨利', '本期淨利', '淨利', '基本每股盈餘'
                     ,'總資產', '總負債', '業主權益', '母公司淨利'
                     ]

def 解析公告財務報告(內容):
    from zhongwen.數 import 取數值
    import pandas as pd
    import numpy as np
    import re
    cols = {'1月1日累計至本期止營業收入(仟元)':'營收'
           ,'1月1日累計至本期止營業毛利(毛損) (仟元)':'毛利'
           ,'1月1日累計至本期止營業利益(損失) (仟元)':'營利'
           ,'1月1日累計至本期止稅前淨利(淨損) (仟元)':'稅前淨利'
           ,'1月1日累計至本期止本期淨利(淨損) (仟元)':'淨利'
           ,'1月1日累計至本期止歸屬於母公司業主淨利(損) (仟元)':'母公司淨利'
           ,'1月1日累計至本期止基本每股盈餘(損失) (元)':'基本每股盈餘'
           }

    def 是否為需要欄位(欄名):
        for c in cols:
            if c in 欄名: return True
        return False
    rs = 內容.split('\n')
    rs = [r.split(':') for r in rs if 是否為需要欄位(r)]
    def 取欄名數值(原始欄名, 數值):
        n = 原始欄名
        v = 取數值(數值)
        try:
            if '仟元' in n:
                v *= 1000
            for c in cols:
                if c in n: 
                    n = cols[c]
            return n, v
        except TypeError:
            return n, np.nan

    rs = [取欄名數值(c, v) for c, v in rs]
    return {c:v for c, v in rs}

def 取財務報告數值(重訊資料):
    from twse_crawler.重大訊息爬蟲 import 抓取重大訊息詳細資料
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    from zhongwen.時 import 上季末, 取正式民國日期
    import pandas as pd
    import numpy as np
    import requests
    import re
    import json

    s = 重訊資料
    公司代號 = s['公司代號']
    公司簡稱 = 查股票簡稱(公司代號 )
    發言日期 = s['發言日期']
    try:
        if 公司代號 == '6103':
            raise KeyError('6103')
        快取資料 = 財務報告數值快取檔[公司代號]
        if 發言日期 <= 快取資料['發言日期']:
            logger.info(f'{公司簡稱}於{取正式民國日期(發言日期)}公告之財報數值較快取落後')
            return 快取資料
    except (KeyError, TypeError): pass
    # if 公司代號 == '6103':
        # breakpoint()
    s['財報日期'] = 上季末
    pat = r'個體|個別|合併'
    if m:=re.search(pat, s.主旨):
        s['財報類型'] = m[0]
    else:
        s['財報類型'] = s.主旨
    try:
        訊息 = json.loads(s.詳細資料)
        for i in range(5):
            try:
                訊息 = 抓取重大訊息詳細資料(訊息)['result']['data'][-1][-1]
                break
            except (requests.exceptions.HTTPError, requests.exceptions.ChunkedEncodingError):
                continue
        r = 解析公告財務報告(訊息)
        for k in r:
            if k in 公告財務報告表欄位:
                s[k] = r[k]
        if pd.notna(s['母公司淨利']) and s['公司代號'] not in ['1465', '2851']:
            s['財報類型'] = '合併'
        else:
            s['財報類型'] = '個別'
        財務報告數值快取檔[公司代號] = s
        return s
    except ValueError as e:
        logger.error(f'抓取{公司代號}公告財務報表資訊發生錯誤：{e}')
        for k in 公告財務報告表欄位:
            s[k] = np.nan
        return s
    except KeyError:
        s['財報類型'] = '個別'
        財務報告數值快取檔[公司代號] = s
        return s

@cache.memoize('探勘公告財務報告表', tag='探勘公告財務報告表', expire=8*60*60)
def 探勘公告財務報告表():
    '目前僅實作探勘本季重大訊息'
    from twse_crawler.重大訊息分析 import 載入近一季重大訊息
    from zhongwen.時 import 季初
    df = 載入近一季重大訊息()
    df = df.query('發言日期>@季初')
    df = df.query('詳細資料.str.contains("otc|sii")')
    df = df.query('not 詳細資料.str.contains("rotc")')
    pat = ".*通過.*(第.*季|年度|年).*財務報(告|表).*|.*(第.*季|年度|年).*財務報(告|表).*通過"
    df = df.query(f'主旨.str.contains("{pat}")')
    df = df.query('not 主旨.str.contains("召開|預計|更正")')
    df = df.apply(取財務報告數值, axis='columns')
    df = df.loc[df.groupby('公司代號')['發言日期'].idxmax()]
    df = df.reset_index(drop=True)
    df.index.name = '序號'
    df.index = df.index+1
    return df[公告財務報告表欄位]

@cache.memoize('取公告股利彙總表', expire=8*60*60)
def 取公告股利彙總表():
    '目前僅實作探勘本季重大訊息'
    from twse_crawler.重大訊息分析 import 載入近一季重大訊息
    from zhongwen.時 import 季初
    df = 載入近一季重大訊息()
    df = df.query('發言日期>@季初')
    df = df.query('詳細資料.str.contains("otc|sii")')
    df = df.query('not 詳細資料.str.contains("rotc")')
    pat = ".*決議.*股利分派.*"
    df = df.query(f'主旨.str.contains("{pat}")')
    df = df.apply(取財務報告數值, axis='columns')
    df = df.loc[df.groupby('公司代號')['發言日期'].idxmax()]
    df = df.dropna(subset='營收')
    df = df.reset_index(drop=True)
    df.index.name = '序號'
    df.index = df.index+1
    return df[公告財務報告表欄位]

def 取股利分派明細(重大訊息):
    from twse_crawler.重大訊息爬蟲 import 抓取重大訊息詳細資料
    from zhongwen.時 import 上季末, 取正式民國日期
    import pandas as pd
    import numpy as np
    import requests
    import re
    import json

    s = 重大訊息
    公司代號 = s['公司代號']
    發言日期 = s['發言日期']
    try:
        快取資料 = 股利公告快取[公司代號]
        if 發言日期 <= 快取資料['發言日期']:
            logger.info(f'{公司代號}於{取正式民國日期(發言日期)}公告之股利較快取落後')
            return 快取資料
    except (KeyError, TypeError): pass
    s['財報日期'] = 上季末
    pat = r'個體|個別|合併'
    if m:=re.search(pat, s.主旨):
        s['財報類型'] = m[0]
    else:
        s['財報類型'] = s.主旨
    try:
        訊息 = json.loads(s.詳細資料)
        for i in range(5):
            try:
                訊息 = 抓取重大訊息詳細資料(訊息)['result']['data'][-1][-1]
                break
            except requests.exceptions.HTTPError:
                continue
        r = 解析公告財務報告(訊息)
        for k in r:
            if k in 公告財務報告表欄位:
                s[k] = r[k]
        if pd.notna(s['母公司淨利']) and s['公司代號'] not in ['1465', '2851']:
            s['財報類型'] = '合併'
        else:
            s['財報類型'] = '個別'
        財務報告數值快取檔[公司代號] = s
        return s
    except ValueError:
        for k in 公告財務報告表欄位:
            s[k] = np.nan
        return s
    except KeyError:
        s['財報類型'] = '個別'
        財務報告數值快取檔[公司代號] = s
        return s

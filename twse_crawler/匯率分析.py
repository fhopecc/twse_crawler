from zhongwen.庫 import 通知執行時間
from diskcache import Cache
from pathlib import Path
import functools

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
cache.clear()

def 設定執行環境():
    from zhongwen.python_dev import 安裝套件
    安裝套件('yfinance')

@functools.cache
@通知執行時間
@cache.memoize('下載美元兌新台幣十年匯率', expire=12*60*60)
def 下載美元兌新台幣十年匯率():
    '主鍵為交易日及匯率'
    from datetime import datetime, timedelta
    from zhongwen.時 import 今日
    import yfinance as yf
    import pandas as pd

    end_date = 今日.to_pydatetime()
    start_date = end_date - timedelta(days=10 * 365)

    data = yf.download( 'USDTWD=X', start=start_date, end=end_date, interval='1d')

    if not data.empty:
        # 通常我們關心 'Close' 價格作為每日匯率
        df = data[['Close']]
        df.columns = ['匯率']
        df.index.name = '交易日'
        df = df.reset_index()
        return df
    else:
        raise Exception(f"未能從 Yahoo Finance 獲取近十年美元兌台幣匯率！")

@functools.cache
@通知執行時間
def 分析美元兌新台幣匯率():
    '''
    分析結果最近匯率資訊，含交易日、匯率、說明、較上季末增減比率。
    '''
    from zhongwen.時 import 上季, 年初, 取正式民國日期
    from zhongwen.數 import 計算增減率
    import zhongwen.時 as 時
    import pandas as pd
    r = 分析結果 = pd.Series()
    h = 美元兌新台幣匯率 = 下載美元兌新台幣十年匯率()
    r = h.iloc[-1]
    t, c = r.交易日, r.匯率
    l = r['上季末匯率'] = h.query('交易日 < @上季.end_time').iloc[-1].匯率
    d = r['較上季末增減比率']= 計算增減率(l, c)
    ly = r['年初匯率'] = h.query('交易日 < @年初').iloc[-1].匯率
    dy = r['較年初增減比率']= 計算增減率(ly, c)
    if t == 時.今日:
        m = f'今日美元兌新台幣{c:,.2f}元'
    else:
        m = f'{取正式民國日期(t)}美元兌新台幣{c:,.2f}元'
    if d > 0:
        m += f'，較上季末增加{d:,.2%}'
    else:
        m += f'，較上季末減少{abs(d):,.2%}'
    if dy > 0:
        m += f'，較年初增加{dy:,.2%}'
    else:
        m += f'，較年初減少{abs(dy):,.2%}'
    r['說明'] = m 
    return r

def 取外銷比重(說明文):
    from zhongwen.數 import 取數值
    import re
    pat = r'外銷比重([\d\.]+[%％])'
    if m:=re.search(pat, 說明文):
        return 取數值(m[1].replace('％', '%'))
    return 0

def 取國外投資比例(說明文):
    '欄位：資料日期、國外投資比例。'
    from zhongwen.數 import 取數值
    from zhongwen.時 import 取日期
    import pandas as pd
    import re
    pat = r'^.*?(\d+年\d+月\d+日).*國外投資比例[為逾]([\d\.]+[%％]).*$'
    r = pd.Series()
    if m:=re.search(pat, 說明文.replace('\n', '')):
        r['資料日期'] = 取日期(m[1])
        r['國外投資比例'] = 取數值(m[2].replace('％', '%'))
    return r

def 分析匯率(股票):
    '''
    一、資料項目：資料日期、分數(0~5)、評語。
    '''
    from 股票分析.人工分析 import 取本人股票筆記
    import pandas as pd
    s, m, d = 0, '', pd.NaT
    try:
        n = 取本人股票筆記(股票).iloc[-1].筆記
        if '外銷比重' in n:
            f = 取外銷比重(n)
            if f >= 0.5:
                r = 分析美元兌新台幣匯率() 
                m = f'外銷比重{f:.2%}，且' + r.說明
                s += r.較上季末增減比率/0.01*f
                d = r.交易日 if pd.notna(r.交易日) else pd.NaT
        elif '國外投資比例' in n:
            資料時間, 國外投資比例 = 取國外投資比例(n)
            i = 國外投資比例 
            if 國外投資比例 > 0.1:
                r = 分析美元兌新台幣匯率() 
                m = f'國外投資比例{i:.2%}，且' + r.說明
                s += r.較上季末增減比率/0.01*i
                d = r.交易日 if pd.notna(r.交易日) else pd.NaT
        else:
            return pd.Series()
    except IndexError:
        return pd.Series()
    s = min(s, 5)
    res = pd.Series()
    res['分數'], res['評語'], res['資料日期'] = s, m+f'({s:.0f})', d
    return res

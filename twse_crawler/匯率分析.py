from zhongwen.庫 import 結果批次寫入
from zhongwen.程式 import 通知執行時間
from zhongwen.時 import 今年數
from diskcache import Cache
from pathlib import Path
import functools
import logging
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
匯率庫 = Path.home() / '.twse_crawler' / '資料庫' / '匯率庫'

@結果批次寫入(匯率庫, '美元兌新台幣匯率', '年度數', list(range(2008, 今年數+1)))
def 抓取年度美元兌新台幣匯率(年度數):
    '''
    一、批號為年度數。
    二、欄位：匯率、交易日。
    三、資料來源：yahoo
    '''
    from datetime import datetime, timedelta
    from zhongwen.時 import 今日
    import yfinance as yf
    import pandas as pd
    logger.info(f"從 Yahoo Finance 抓取{年度數}年美元兌台幣匯率！")
    data = yf.download('USDTWD=X', start=datetime(年度數,1,1)
                      ,end=datetime(年度數,12,31)
                      ,interval='1d')
    if not data.empty:
        df = data[['Close']]
        df.columns = ['匯率']
        df.index.name = '交易日'
        df = df.reset_index()
        df = df.sort_index()
        df['年度數'] = 年度數
        return df
    else:
        raise Exception(f"未能從 Yahoo Finance 獲取{年度數}年美元兌台幣匯率！")

@cache.memoize('取美元匯率', expire=24*60*60)
def 取美元匯率():
    '''
    一、索引為 pd.DatetimeIndex。
    二、欄位：匯率、交易日。
    三、每日更新。
    '''
    from zhongwen.庫 import 批次載入
    from zhongwen.時 import 昨日
    df = 批次載入(匯率庫, '美元兌新台幣匯率', '年度數', 時間欄位='交易日', 起始批號=2008).sort_values('交易日')
    df = df.set_index('交易日')
    最近日期 = df.index.max()
    if 最近日期 < 昨日:
        抓取年度美元兌新台幣匯率(昨日.year)
        df = 批次載入(匯率庫, '美元兌新台幣匯率', '年度數', 時間欄位='交易日', 起始批號=2008).sort_values('交易日')
        df = df.set_index('交易日')
    return df.sort_index()

@cache.memoize('預測次年底美元匯率', expire=24*60*60)
def 預測次年底美元匯率():
    '''
    一、歷日值索引須是 DatetimeIndex。
    二、結果項目：預估值、預估季均值、預估方法說明。
    三、預估季均值：歷史與次年度各季之季均值、季起日值、季迄日值、季增減數。
    '''
    from twse_crawler.預估次年底 import 預估次年底值, 表達期間
    from zhongwen.時 import 取正式民國日期
    m = 預估次年底值(取美元匯率().匯率)
    r = m[["預估值", "預估季均值"]]
    r['預估方法說明'] = (
        f'依{表達期間(m.最近歷史值時間)}前{m.歷史值數量:,}日美元匯率訓練'
        f'得回測{m.回測資料數:,}日之變動範圍為{m.rmse:,.2f}元，比率{m.mape:,.2%}'
        f'{m.模型名稱}模型，'
        f'預估至{m.最後預估值時間.year-1911:,}年底之{m.預估值數量:,}日美元匯率'
        )
    return r

@cache.memoize('以匯率預測次年底業外損益', expire=24*60*60)
def 以匯率預測次年底業外損益(股票):
    """
    一、傳回預估各季值、預估說明、預估方法說明
    二、預估各季值含歷季值。
    """
    from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    from twse_crawler.預估次年底 import 表達預測準確度
    from twse_crawler.預估次年底 import 表達預估方法, 表達預估說明
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.表 import 表示
    預估季均匯率結果 = 預測次年底美元匯率()
    預估季均匯率 = 預估季均匯率結果.預估季均值[['季均值', '季增減數']]
    df = 取財報彙總表(股票).set_index('財報日期').to_period('Q')
    r = 依外部季數據預估次年底數值(df.業外損益, 預估季均匯率, 單位="元")
    r['預估方法說明'] = (f'{預估季均匯率結果.預估方法說明}，併同{表達預估方法(r)}'
                        )
    r['預估說明'] = 表達預估說明(r, '業外損益')
    return r

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

    data = yf.download('USDTWD=X', start=start_date, end=end_date, interval='1d')

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

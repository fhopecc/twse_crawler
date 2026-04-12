from zhongwen.庫 import 通知執行時間, 增加定期更新
from zhongwen.快取 import 增加快取時序分析結果
from diskcache import Cache, Index
from pathlib import Path
import logging
import functools
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'twse_crawler' / 'cache' / Path(__file__).stem)
預測報酬率結果快取檔 = Index(str(Path.home() / 'twse_crawler' / 'cache' / '預測報酬率結果快取檔'))

def 抓取近一週上市櫃收盤行情():
    from zhongwen.時 import 一日, 一週前, 今日, 是工作日, 最近工作日, 取正式民國日期
    from twse_crawler.證交所爬蟲 import 抓取上市每日收盤行情
    from twse_crawler.櫃買中心爬蟲 import 抓取上櫃股票行情
    from zhongwen.表 import 顯示
    import pandas as pd
    d = 一週前
    while d <= 最近工作日: 
        if 是工作日(d):
            logger.info(f'抓取{取正式民國日期(d)}收盤行情')
            抓取上市每日收盤行情(d)
            抓取上櫃股票行情(d)
        d += 一日

@functools.cache
@通知執行時間
@cache.memoize('取最近上市櫃收盤行情', expire=12*60*60)
def 取最近上市櫃收盤行情(股票=None):
    '''
    一、證券代號、交易日期，收盤價。
    二、無最近上市櫃收盤行情者，擲出數據不足例外。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from twse_crawler.證交所爬蟲 import 上市每日收盤行情庫
    from twse_crawler.櫃買中心爬蟲 import 上櫃股票行情庫
    from zhongwen.庫 import 批次載入
    from zhongwen.時 import 半月前
    from zhongwen.表 import 顯示, 數據不足
    import pandas as pd

    if 股票:
        股票代號 = 查股票代號(股票)
        股票簡稱 = 查股票簡稱(股票)
        df = 取最近上市櫃收盤行情()
        df = df.query('證券代號==@股票代號')
        if df.empty:
            raise 數據不足(f'{股票簡稱}最近上市櫃收盤行情', 0, 1) 
        return  df

    抓取近一週上市櫃收盤行情()    
    columns = ['交易日期', '證券代號', '證券名稱', '開盤價', '最高價', '最低價', '收盤價']
    df1 = 批次載入(上市每日收盤行情庫, '上市每日收盤行情表', '交易日期', '交易日期', 起始批號=半月前)
    df1 = df1[columns]
    df2 = 批次載入(上櫃股票行情庫, '上櫃股票行情表', '交易日期', '交易日期', 起始批號=半月前)
    df2 = df2[['交易日期', '代號', '名稱', '開盤', '最高', '最低', "收盤"]]
    df2.columns = columns
    df = pd.concat([df1, df2])
    df = df.dropna(subset='收盤價')
    df = df.query('收盤價>0')
    def 取個股最近交易(個股交易):
        return 個股交易.iloc[-1]
    df = df.groupby('證券代號', as_index=False).apply(取個股最近交易)
    df = df.reset_index(drop=True)
    return df

def 組現流折現式字串(預計股利):
    ds = 預計股利
    if len(ds) == 0: return ''
    if len(ds) == 1: return f'{ds[0]}/r'
    if len(ds) == 2: return f'{ds[0]}+({ds[1]}/r)/(1+r)'
    s  = [f'{ds[0]}', f'{ds[1]}/(1+r)']
    s += [f'{d}/(1+r)**{i+2}' for i, d in enumerate(ds[2:-1])]
    s += [f'({ds[-1]}/r)/(1+r)**{len(ds)-1}']
    return '+'.join(s)
 
def 符號計算股票內部報酬率(股價, 預計股利, 每年發放次數=1):
    # 股利現流為負數，即傳回0
    # if any(股利 < 0 for 股利 in 預計股利): return 0
    from sympy import symbols, Eq, solve, sympify
    from sympy.core.sympify import SympifyError
    p = 股價
    ds = 預計股利
    r = symbols('r')
    try:
        f = sympify(組現流折現式字串(預計股利))
    except SympifyError:
        logger.error('無法預測股利')
        return 0
    eq = Eq(f, 股價)
    ss = solve(eq, r)
    try:
        s0 = [float(s) for s in ss if s.is_real and float(s) > 0][0]
    except IndexError:
        return 0
    return s0*每年發放次數

def 數值計算股票內部報酬率(股價, 股利, 每年發放次數=1, 預測股價=None):
    # 因目前上市櫃、興櫃及公發公司合計2,478間，爰衡量執行3,000次所需秒數：
    # brentq：  0.05
    # fsolve：  0.46
    # sympy ：155.74
    # brentq 較 sympy 快速2,928倍。
    # 精度 sympy > brentq > fsolve 
    from scipy.optimize import brentq, fsolve
    p  = 股價
    if 預測股價:
        ep = 預測股價
    else:
        ep = p
    ds = 股利
    if len(股利) == 2:
        # brentq：1,000次費0.02秒，
        # fsolve: 1,000次費0.11秒，
        # 且 brentq 較 fsolve 精確1位。
        d1, d2 = ds
        def f2(r):
            return -p+d1+(d2/r)/(1+r)
        try:
            return brentq(f2, 0.0001, 2) * 每年發放次數
        except ValueError:
            return float('nan')
    def fn(r):
        s = -p
        for i, d in enumerate(ds):
            if i == len(ds)-1:
                s+=(d/r)/(1+r)**i
            elif i == 0:
                s+=d 
            else:
                s+=d/(1+r)**i
        return s
    # return fsolve(fn, 0.06)[0]
    try:
        return brentq(fn, 0.000001, 10) * 每年發放次數
    except ValueError:
        return 符號計算股票內部報酬率(股價, 股利, 每年發放次數)

def 取現價(內部報酬率, 股利):
    '假設最後一筆股利之後股利均以最後一筆設算'
    from collections.abc import Iterable 
    r, ds = 內部報酬率, 股利

    if isinstance(ds, str) or not isinstance(ds, Iterable):
        ds = [ds]
    s = 0
    for i, d in enumerate(ds):
        if i == len(ds)-1:
            s+=(d/r)/(1+r)**i
        elif i == 0:
            s+=d 
        else:
            s+=d/(1+r)**i
    return s
    

@通知執行時間
@cache.memoize(tag='行情分析', expire=12*60*60)
def 行情分析():
    '索引為代號'
    from 股票分析.撿股讚爬蟲 import 抓取股票行情頁
    from zhongwen.number import 轉數值
    from io import StringIO
    import pandas as pd
    c = 抓取股票行情頁()
    converters = {'漲跌幅':轉數值
                 ,'本周漲跌幅':轉數值
                 }
    df = pd.read_html(StringIO(c), converters=converters)[0]
    df2 = 上市櫃收盤行情()
    df = df.merge(df2, how='left', left_on='代號', right_on='證券代號')
    df.set_index('代號', inplace=True)
    # df['K線圖'] = df.apply(kline, axis=1)
    # df['行情分析評分'] = df.apply(評分, axis=1)
    # df['行情分析加減項'] = df.apply(加減分項, axis=1)
    # df['行情概述'] = df.apply(概述, axis=1)
    return df

def 概述(r):
    from zhongwen.number import 增減百分比
    漲跌 = ["漲", "跌"]
    return f'現價{r.股價:.2f}元，{增減百分比(r.漲跌幅, 漲跌)}，本週{增減百分比(r.本周漲跌幅, 漲跌)}'

def kline(r):
    from zhongwen.number import 轉數值
    svg = '<svg style="align:right;display:block" width=9 height=20>'
    o = 轉數值(r.開盤價)
    c = 轉數值(r.收盤價)
    h = 轉數值(r.最高價)
    l = 轉數值(r.最低價)
    t = h-l
    if c>o:
        color='red'
        try:
            y = (h-c)/t*20
        except:
            y = 0
    else:
        color='green'
        try:
            y = (h-o)/t*20
        except:
            y = 0
    try:
        lheight = abs(c-o)/t*20
        if lheight==0:
            lheight=1
    except:
        lheight = 1
    svg += f'<rect width=3 height=20 x=3 fill="{color}"/>'
    svg += f'<rect width=9 height={lheight} y={y} fill="{color}"/>'
    svg += r'<\svg>'
    return svg


@functools.cache
@通知執行時間
@增加快取時序分析結果(取最近上市櫃收盤行情, '股票', '交易日期', 預測報酬率結果快取檔)
def 預測報酬率(股票, 重新分析=False):
    '''
    一、預測股票前年至次年股利及次年底股價推算內部報酬率。
    二、以人工估算合理淨利估算次年股利合理值，無則遞以預測值，除殖利率設算次年底股價。
    三、資料項目：股票、報酬率、預測報酬率說明、交易日期、收盤價、前年至次年股利、
                  除息交易日、股利發放日。
    四、股票可為股票名稱或股票代號。
    五、快取分析結果並可指定重新分析。
    六、無上市櫃交易資料者，因股價須運用成本或其他資料估算，爰擲出數據不足例外。
    七、說明項目：預測報酬率、現金股利發放日期、金額及匯入帳戶及半月前預告通知、
                  是否除息。
    八、傳回 Series。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from zhongwen.數 import 取最簡約數, 計算增減率, 增減百分點
    from zhongwen.時 import 取正式民國日期, 今日, 年初
    from twse_crawler.股利分析 import 預測股利, 取除權息概述
    from 財務.股票 import 取股票帳戶資訊
    from zhongwen.表 import 數據不足
    from diskcache import Index
    import pandas as pd
    import numpy as np
    import re
    r = 預測結果 = pd.Series()
    證券代號, 股票簡稱 = 查股票代號(股票), 查股票簡稱(股票)

    try:
        最近行情 = 取最近上市櫃收盤行情(股票).iloc[-1]
    except 數據不足 as e:
        raise 數據不足(e.名稱, e.實際筆數, e.至少筆數, '預測報酬率')

    交易日期 = 最近行情.交易日期
    收盤價 = 最近行情.收盤價 
    try:
        預測股利結果 = 預測股利(股票)
        前年至次年股利 = 預測股利結果.前年至次年股利
        預測股利說明 = 預測股利結果.預測股利說明
        除息交易日 = 預測股利結果.除息交易日
        除權交易日 = 預測股利結果.除權交易日
        現金股利發放日 = 預測股利結果.現金股利發放日
        次年底股價 = 前年至次年股利.iloc[-1]*收盤價/前年至次年股利.iloc[0]
    except 數據不足 as e:
        e.目的 = f'{e.目的}及報酬率'
        raise e

    msg = f'{預測股利說明}，'
    if isinstance(前年至次年股利, pd.Series):
        msg += f'依上開股利預測值及{取正式民國日期(交易日期)}收盤{收盤價:.2f}元'
        msg += f'推算次年底股價{次年底股價:.2f}元'
        if (percent := abs(計算增減率(收盤價, 次年底股價))) > 0:
            msg += f'，{增減百分點(percent)}％，'
        else:
            msg += f'，'
        if pd.notna(除息交易日) and 除息交易日>年初 and 除息交易日 < 今日:
            報酬率 = 數值計算股票內部報酬率(收盤價
                   ,list(前年至次年股利.fillna(0).tail(2).values))
            msg += f'並因已除息'
            if pd.notna(現金股利發放日):
                if 現金股利發放日 < 今日:
                    msg += f'且已於{取正式民國日期(現金股利發放日)}發放股利'
                else:
                    msg += f'且將於{取正式民國日期(現金股利發放日)}發放股利'
            msg += f'，爰計算二年報酬率{報酬率:.2%}'

        else:
            報酬率 = 數值計算股票內部報酬率(收盤價
                   ,list(前年至次年股利.fillna(0).tail(3).values))
            if pd.notna(除息交易日) and 除息交易日>年初 and (delta:=(除息交易日 - 今日).days) < 14:
                msg = f"將於{delta}日後{取正式民國日期(除息交易日)}除息，{msg}"
            msg += f'推算三年報酬率{報酬率:.2%}'
    else:
        報酬率 = np.nan
        msg += f'收盤{收盤價:.2f}元，無股利資料計算報酬率'

    股票帳戶資訊 = 取股票帳戶資訊(股票)
    if not 股票帳戶資訊.empty:
        股票帳戶資訊 = 股票帳戶資訊.iloc[-1]
        淨股利, 應收取現金股利 = 股票帳戶資訊.淨股利, 股票帳戶資訊.應收取現金股利
        股利收取日期, 股利匯入帳戶 = 股票帳戶資訊.日期, 股票帳戶資訊.股利匯入帳戶 
        if pd.notna(股利收取日期) and 股利收取日期>=現金股利發放日:
            m  = f"現金股利{取最簡約數(淨股利)}元"
            m += f"已於{取正式民國日期(股利收取日期)}匯入{股利匯入帳戶}；{msg}"
            msg = m
        elif 現金股利發放日 == 今日:
            m =  f"現金股利{應收取現金股利:,.0f}元應於今日"
            m += f"匯入{股利匯入帳戶}，請確認是否匯入並登帳；{msg}"
            msg = m
        elif 現金股利發放日>年初 and 現金股利發放日 < 今日:
            m =  f"現金股利{應收取現金股利:,.0f}元應於{取正式民國日期(現金股利發放日)}"
            m += f"匯入{股利匯入帳戶}，請確認是否匯入並登帳；{msg}"
            msg = m
        elif 現金股利發放日>年初 and (delta:=(現金股利發放日 - 今日).days) < 14:
            m = f"現金股利{應收取現金股利:,.0f}元"
            m += f"將於{delta}日後{取正式民國日期(現金股利發放日)}"
            m += f"匯入{股利匯入帳戶}；{msg}"
            msg = m
    r['股票'] = 股票
    r['報酬率'] = 報酬率
    r['預測報酬率說明'] = msg
    r['收盤價'] = 收盤價
    r['前年至次年股利'] = 前年至次年股利
    r['交易日期'] = 交易日期
    r['除息交易日'] = 除息交易日
    r['除權交易日'] = 除權交易日
    r['現金股利發放日'] = 現金股利發放日
    return 預測結果

if __name__ == '__main__':
    r = 數值計算股票內部報酬率(1090, [0, 33.4, 36.71])
    print(r)
    v = 取現價(0.03, [0, 33.4, 36.71])
    print(v)

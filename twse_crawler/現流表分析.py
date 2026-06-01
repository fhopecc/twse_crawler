from zhongwen.程式 import 通知執行時間
from diskcache import Cache
from pathlib import Path
import functools
import logging

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

@functools.cache
@通知執行時間
@cache.memoize('取累積現流表', expire=12*60*60)
def 取累積現流表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報類型、財報日期。
    二、欄位：營運產生之現金流入（流出）、營業活動之淨現金流入（流出）、
              取得不動產、廠房及設備、投資活動之淨現金流入（流出）、
              籌資活動之淨現金流入（流出）、
              匯率變動對現金及約當現金之影響、本期現金及約當現金增加（減少）數
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.庫 import 批次載入
    from twse_crawler import 財報爬蟲
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取累積現流表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')
    df = 批次載入(財報爬蟲.現流表資料庫, '現流表', '財報日期', '財報日期')
    df = df.drop_duplicates(subset=["股票代號", "財報類型", "財報日期"], keep='first')
    if 個體報表:
        df = df.query("財報類型=='個體'")
    else:
        df = df.query("not 財報類型=='個體'")
    df = df.sort_values(by=["股票代號", "財報日期"])
    return df

def 取單期淨額(歷史累積金額):
    '''
    一、與上期年度相異者，即第1期，單期淨額同累積金額。
    二、其餘期別較上期差異數即為單期淨額。
    三、依與上期財報相距月數推論財報期別頻率為季、半年或年。
    '''
    from zhongwen.表 import 表示
    import pandas as pd
    import sys
    s = 歷史累積金額
    應顯示細節者 = s.iloc[0].name == 'abcd'
    小數欄 = s.select_dtypes(include=['float64']).columns
    非小數欄 = s.select_dtypes(exclude=['float64']).columns
    n = s[小數欄]

    本期數 = n
    較上期差異數 = n - n.shift()
    是否與上期同年度 = s.財報日期.dt.year == s.財報日期.shift().dt.year
    n = (較上期差異數.multiply(是否與上期同年度, axis=0).fillna(0) + 
         本期數.multiply(~是否與上期同年度, axis=0)
        )
    r = pd.concat([s[非小數欄], n], axis=1) 
    try:
        最新財報日期 = s['財報日期'].iloc[-1]
        上期財報日期 = s['財報日期'].iloc[-2]
        與上期財報相距月數 = (最新財報日期.year - 上期財報日期.year) * 12 + \
                             (最新財報日期.month - 上期財報日期.month)

        if 與上期財報相距月數 == 3:
            r['財報期別'] = r.財報日期.map(lambda d: pd.Period(d, 'Q-DEC'))
            r['頻率'] = 'Q-DEC'
        elif 與上期財報相距月數 == 6:
            r['財報期別'] = r.財報日期.map(lambda d: pd.Period(d, '6M'))
            r['頻率'] = '6M'
        else:
            r['財報期別'] = r.財報日期.map(lambda d: pd.Period(d, 'Y'))
            r['頻率'] = 'Y'
    except IndexError:
        r['財報期別'] = r.財報日期.map(lambda d: pd.Period(d, 'Q-DEC'))
        r['頻率'] = 'Q-DEC'
    r['股票代號'] = s.name
    if 應顯示細節者: sys.exit()
    return r

@functools.cache
@通知執行時間
@cache.memoize('取現流表', expire=15*24*60*60)
def 取現流表(股票=None, 個體報表=False):
    '''
    一、依累積現流表設算單期現流表。
    二、主鍵：股票代號、財報日期及財報類型。
    三、欄位：營運產生之現金流入（流出）、營業活動之淨現金流入（流出）、
              取得不動產、廠房及設備、投資活動之淨現金流入（流出）、
              籌資活動之淨現金流入（流出）、
              匯率變動對現金及約當現金之影響、本期現金及約當現金增加（減少）數
    四、取指定「股票」之合併及個別歷史現流表，未指定則取全部股票歷史現流表。
    五、來源為財報檔之現流表。
    六、排除逾一年未公布財報者。
    七、依累季前後期數據差異推算單季數據。
    八、可取指定「個體報表」之個體歷史現流表。
    九、僅限按季公布財報者。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    import pandas as pd
    import numpy as np

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取現流表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')

    df = 取累積現流表(個體報表=個體報表)
    # df = df.groupby(['股票代號']).apply(取單期淨額).reset_index(level=0)
    df = df.groupby(['股票代號'], group_keys=False).apply(取單期淨額)
    df = df.query('頻率=="Q-DEC"')
    df['財報季度'] = df.財報日期.dt.to_period('Q')
    return df

def 取移動年度加總(季數據):
    df = 季數據
    df = df.sort_values('財報日期') 
    ncols = df.select_dtypes(include=['number']).columns
    df[ncols] = df[ncols].rolling(window=4).sum()
    return df

@functools.cache
@通知執行時間
@cache.memoize('取移動年度現流表', expire=15*24*60*60)
def 取移動年度現流表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報類型、財報日期。
    二、欄位：自由現金流、營運產生之現金流入（流出）、營業活動之淨現金流入（流出）、
              取得不動產、廠房及設備、投資活動之淨現金流入（流出）、
              籌資活動之淨現金流入（流出）、
              匯率變動對現金及約當現金之影響、本期現金及約當現金增加（減少）數。
    三、自由現金流 = 營業活動之淨現金流入 + 取得不動產、廠房及設備 + 
                     短期借款增加 + 短期借款減少 + 發行公司債 + 償還公司債 +
                     舉借長期借款 + 償還長期借款 + 其他借款增加 + 其他借款減少
    四、上式以加法合併各現流表項目，係因各項已依現金流入及流出分別以正數及負數表示。
    五、前式係參考巴菲特定義股東自由現金流之原則設計。
    交、僅限按季公布財報者。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.現流表分析 import 取現流表
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取移動年度現流表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')

    df = 取現流表(個體報表=個體報表)
    try:
        df = df.groupby(['股票代號']).apply(取移動年度加總).reset_index(level=0)
    except ValueError: 
        df = df.groupby(['股票代號'], group_keys=False).apply(取移動年度加總)

    df = df.fillna(0)
    df['自由現金流'] = df['營業活動之淨現金流入（流出）'] + df['取得不動產、廠房及設備'] + \
                       df['短期借款增加'] + df['短期借款減少'] + \
                       df['發行公司債'] + df['償還公司債'] + \
                       df['舉借長期借款'] + df['償還長期借款'] + \
                       df['其他借款增加'] + df['其他借款減少']
    return df

@functools.cache
@通知執行時間
@cache.memoize('取近年現流表', expire=15*24*60*60)
def 取近年現流表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報日期及財報類型。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.損益表分析 import 取近年總額
    import pandas as pd
    import numpy as np
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取近年現流表()
        return df.query('股票代號==@股票代號')
    df = 取現流表(個體報表=個體報表).query('頻率=="Q-DEC"')
    df = df.sort_values(by=["股票代號", "財報日期"])
    df = df.groupby(['股票代號'], as_index=False).apply(取近年總額)
    return df


@functools.cache
@通知執行時間
@cache.memoize('取年度現流表', expire=12*60*60)
def 取年度現流表(股票=None, 個體報表=False):
    '''
    一、即取第四季累積現流表。
    二、指定股票則傳回該股票年度現流表。
    三、主鍵為股票代號、財報日期及財報類型。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.庫 import 批次載入
    from twse_crawler import 財報爬蟲
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取年度現流表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')
    df = 取累積現流表(個體報表=個體報表)
    df = df.query('財報日期.dt.month==12')
    return df

def 營運現金需求(tifrs):
    '估計企業全年營運現金需求'
    '張明輝，2019年，第87及88頁'
    '營運現金需求=營收-淨利+折舊+攤銷-設備支出-發放股利-支付利息'
    '如非第4季財報，傳回前一年度數值'
    try:
        return (tifrs.營收 
               -tifrs.損益 
               +tifrs.折舊費用
               +tifrs.攤銷費用
               -tifrs.廠房設備支出
               -tifrs.發放現金股利
               -tifrs.支付利息
                ) 
    except Exception as e:
        raise ValueError(f'operating_expenses error occurred:{e}')

def 現金水位(tifrs):
    '現金水位：估計現金支應月數'
    '張明輝，2019年，第87及88頁'
    '現金/月營運現金需求'
    return tifrs.現金/(營運現金需求(tifrs)/12)

def 分析現流表結構(股票):
    from twse_crawler.財報分析 import 取財報彙總表
    df = 取財報彙總表(股票)
    df['自由現金流'] = df['營業活動之淨現金流入（流出）'] + df['取得不動產、廠房及設備']
    return df

def 分析自由現金流(股票):
    '''
    一、表達項目：評語、分數
    二、本季自由現金流入較去年同季每增減1個百分點則分別增減10分，惟上下限為正負1,000分，
        流出扣減1,000分。
    三、年度自由現金流入較去年每增減1個百分點則分別增減連續增減次數10倍之分數，
        惟上下限為正負5,000分，流出則為負5,000分，由流出轉為流入因趨勢未明顯則為100分。
    '''
    from twse_crawler.趨勢分析 import 分析本季同比, 分析同比差異主次因, 年化趨勢評分
    from twse_crawler.趨勢分析 import 分析歷年數據增減情形
    from twse_crawler.淨利趨勢分析 import 分析營利
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取最簡約數
    import pandas as pd
    import math

    df = 取累積現流表(股票)
    if df['取得不動產、廠房及設備'].notna().any():
        try:
            df['取得不動產、廠房及設備'] = df['取得不動產、廠房及設備'].fillna(0)
            df['自由現金流'] = df['營業活動之淨現金流入（流出）'] + df['取得不動產、廠房及設備']
            r1 = 分析本季同比(df, '自由現金流', '財報日期')
            try:
                r2 = 分析同比差異主次因(df, 股票, '自由現金流'
                                     , ['營業活動之淨現金流入（流出）', '取得不動產、廠房及設備'])
                評語 = r1.說明
                if r2.主因 == '營業活動之淨現金流入（流出）':
                    r3 = 分析營利(股票)
                else: # 其他主因 
                    r3 = 分析本季同比(df, r2.同比差異主因, '財報日期', 表達數據季別=False) 
                評語 = r3.說明.replace('，係', f'，使{評語}，源自')
            except 數據不足 as e:
                logger.error(e)
            if math.isinf(r1.同比):
                分數 = 100 if r1.同比 > 0 else -1000
            else:
                分數 = max(-1000, min(1000, 1000*r1.同比)) if r1.本季 > 0 else -1000
        except (AttributeError, 數據不足) as e:
            logger.error(e)
            評語, 分數 = '', 0
    else:
        評語, 分數 = '', 0

    df = 取年度現流表(股票)
    if df['取得不動產、廠房及設備'].notna().any():
        df['取得不動產、廠房及設備'] = df['取得不動產、廠房及設備'].fillna(0)
        df['自由現金流'] = df['營業活動之淨現金流入（流出）'] + df['取得不動產、廠房及設備']
        r2 = 分析歷年數據增減情形(df, '自由現金流', '財報日期')
        評語 += f'，{r2.說明}' if len(評語) > 0 else r2.說明
        try:
            分數 = 年化趨勢評分(r2.本年, r2.環比, r2.連續增減次數, 統計期間='年')
        except ValueError:
            分數 = 0
        
    res = pd.Series()
    if len(評語) > 0:
        try:
            res['分數'] = int(分數)
        except Exception:
            res['分數'] = 0
        res['評語'] = 評語 + f'{res.分數:.0f}'
    return res

def 分析自由現金流趨勢(股票=None):
    '''
    一、自由現金流 = 營業活動之淨現金流入 + 取得不動產、廠房及設備 + 
                     短期借款增加 + 短期借款減少 + 發行公司債 + 償還公司債 +
                     舉借長期借款 + 償還長期借款 + 其他借款增加 + 其他借款減少
    二、上式以加法合併各現流表項目，係因各項已依現金流入及流出分別以正數及負數表示。
    三、前式係參考巴菲特定義股東自由現金流之原則設計。
    四、欄位：評語、評分(5)
    '''
    from twse_crawler.趨勢分析 import 分析歷季數據增減情形
    from zhongwen.表 import 顯示
    import pandas as pd
    cs = 歷季移動年度現流表 = 取移動年度現流表(股票)
    r0 = 分析歷季數據增減情形(cs, '自由現金流', '財報日期')
    s, m = 0, ''
    if not r0.empty:
        if r0.同比 > 0:
            s = min(( r0.同比*abs(r0.連續增減次數) ) / 0.1, 5)
        m = r0.說明
    r0['分數'] = s
    r0['評語'] = m + f'({r0.分數:.0f})'
    return r0

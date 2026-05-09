from zhongwen.庫 import 通知執行時間, 增加定期更新
from zhongwen.快取 import 增加快取時序分析結果
from twse_crawler.財報爬蟲 import 爬取上季財報
from diskcache import Cache, Index
from pathlib import Path
import functools
import logging

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

損益表分析結果快取檔 = Index(str(Path.home() / 'cache' / '損益表分析結果快取檔'))

@functools.cache
@通知執行時間
@cache.memoize('取累積損益表', expire=24*60*60)
def 取累積損益表(股票=None):
    '''
    一、指定股票則傳回該股票歷史累積損益表。
    二、取財報檔之損益表，另以重大訊息公告損益表補充，並排除逾一年未公布財報者。
    三、主鍵為股票代號、財報類型及財報日期。
    '''
    from twse_crawler.重大訊息分析 import 探勘公告財務報告表, cache
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.number import 除零例外則回覆非數常數
    from zhongwen.快取 import 刪除指定名稱快取
    from twse_crawler.財報爬蟲 import 損益資料庫
    from collections.abc import Iterable 
    from zhongwen.庫 import 批次載入
    from zhongwen.時 import 一年前
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    # 刪除指定名稱快取(cache, '探勘公告財務報告表')

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取累積損益表()
        return df.query('股票代號==@股票代號')

    renames = {'營業收入合計':'營收'
              ,'營業毛利（毛損）':'毛利'
              ,'營業利益（損失）':'營利'
              ,'本期稅前淨利（淨損）':'稅前淨利'
              ,'本期淨利（淨損）':'淨利'
              ,'母公司業主（淨利／損）':'母公司淨利'
              ,'基本每股盈餘合計':'基本每股盈餘'
              ,'稀釋每股盈餘合計':'稀釋每股盈餘'
              } 
    df = 批次載入(損益資料庫, '損益表', '財報日期', '財報日期').rename(columns=renames)
    df['資訊來源'] = '財報檔'

    # 補正重大訊息公告數據，重大訊息不分合併及個別(如青鋼)，
    # 故合併及個別均轉為 N，意味非個體報表
    dfset = df.股票代號+df.財報日期.astype(str)+df.財報類型.map({"合併":'N', "個別":"N", "個體":"Y"})
    
    try:
        df0 = 探勘公告財務報告表()
        df0.rename(columns={'公司代號':'股票代號'}, inplace=True)
        df0['資訊來源'] = '重大訊息'
        df0['財報識別'] = df0.股票代號+df0.財報日期.astype(str)+df0.財報類型.map({"合併":'N', "個別":"N", "個體":"Y"})
        df0 = df0.query('not 營收.isna()')
        df0 = df0.query('not 財報識別 in @dfset')
        del df0['財報識別']
        df = pd.concat([df, df0])
    except KeyError:
        logger.error('尚無重大訊息公告損益表數據！')
    df.drop_duplicates(subset=['財報日期', '股票代號', '財報類型'], keep='first', inplace=True)
    股票最近財報日期 = df.groupby('股票代號').財報日期.max()

    逾一年未公布財報股票代號 = 股票最近財報日期[股票最近財報日期<一年前].index
    df = df.query('not 股票代號.isin(@逾一年未公布財報股票代號)')
    df.sort_values('財報日期', inplace=True)

    df['營收'] = df.營收.fillna(df['淨收益']) # 金控
    df['營收'] = df.營收.fillna(df.收益合計) # 統一證等
    df['營收'] = df.營收.fillna(df.收入合計) # 新纖等
    df['營收'] = df.營收.replace(0, np.nan) # 以nan取代0，使作除數時不丢出除零例外
    df['營利'] = df.營利.fillna(df['營業利益'])
    df['稅前淨利'] = df.稅前淨利.fillna(df['繼續營業單位稅前淨利（淨損）'])
    df['淨利'] = df['母公司淨利'].fillna(df.淨利).fillna(df['本期稅後淨利（淨損）'])
    df['每股盈餘'] = df.基本每股盈餘
    df['業外損益'] = df.稅前淨利 - df.營利
    df['業外比重'] = df.apply(lambda r: r.業外損益/r.稅前淨利 if r.稅前淨利>0 else np.inf
                             ,axis='columns')
    return df               

@functools.cache
@通知執行時間
@cache.memoize('取損益表', expire=15*24*60*60)
def 取損益表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報日期及財報類型。
    二、欄位：營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益、股數。
    三、預設傳回全部股票歷史合併或個別損益表，亦可傳回指定股票歷史損益表。
    四、來源為財報檔之損益表，並補正重大訊息公告財務報告摘要。
    五、排除逾一年未公布財報者。
    六、依累季前後期數據差異推算單季數據。
    七、亦能指定「個體報表」選項傳回個體損益表。
    八、以淨利除以每股盈餘設算「股數」。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.現流表分析 import 取單期淨額
    from zhongwen.表 import 表示
    import pandas as pd
    import numpy as np

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取損益表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')

    df = 取累積損益表()
    
    if 個體報表:
        df = df.query("財報類型=='個體'")
    else:
        df = df.query("not 財報類型=='個體'")

    df = df.sort_values(by=["股票代號", "財報日期"])
    # 表示(df, 顯示索引=True)
    # df = df.groupby(['股票代號'], as_index=False).apply(取單期淨額)
    # group_keys=False 是不把分組鍵不加入 apply 彙總結果之多重索引。
    df = df.groupby(['股票代號'], group_keys=False).apply(取單期淨額)
    df = df.query('頻率=="Q-DEC"')

    df['營收'] = df.營收.fillna(df['淨收益']) # 金控
    df['營收'] = df.營收.fillna(df.收益合計) # 統一證等
    df['營收'] = df.營收.fillna(df.收入合計) # 新纖等
    df['營利'] = df.營利.fillna(df['營業利益'])
    df['稅前淨利'] = df.稅前淨利.fillna(df['繼續營業單位稅前淨利（淨損）'])
    df['淨利'] = df['母公司淨利'].fillna(df.淨利).fillna(df['本期稅後淨利（淨損）'])
    df['每股盈餘'] = df.基本每股盈餘
    df['股數'] = df.淨利/df.每股盈餘
    df['營收'] = df.營收.replace(0, np.nan) # 為不丢出除零例外，將 0 以 nan 取代，
    df['成本'] = df.營收 - df.毛利
    df['費用'] = df.毛利 - df.營利
    df['業外損益'] = df.稅前淨利 - df.營利
    df['業外比重'] = df.apply(lambda r: r.業外損益/r.稅前淨利 if r.稅前淨利>0 else np.inf
                             ,axis='columns')
    df['其他損益'] = df.淨利- df.稅前淨利
    df['毛利率'] = df.毛利/df.營收
    df['營利率'] = df.營利/df.營收
    df['稅前淨利率'] = df.稅前淨利/df.營收
    df['淨利率'] = df.淨利/df.營收
    df['營利率'] = df.營利率.fillna(df.稅前淨利率)
    df['營收'] = df.營收.fillna(0)
    return df

@functools.cache
@通知執行時間
@cache.memoize('取移動年度損益表', expire=15*24*60*60)
def 取移動年度損益表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報類型、財報日期。
    二、欄位：營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益
    三、僅限按季公布財報者。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.現流表分析 import 取移動年度加總
    from zhongwen.表 import 表示
    import pandas as pd
    import numpy as np

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取移動年度損益表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')

    df = 取損益表(個體報表=個體報表)
    # 表示(df)
    try:
        df = df.groupby(['股票代號']).apply(取移動年度加總).reset_index(level=0)
    except ValueError: 
        df = df.groupby(['股票代號'], group_keys=False).apply(取移動年度加總)
    # 表示(df)
    return df

def 取近年總額(歷史財報):
    '近一年總額'
    import numpy as np
    df = 歷史財報.tail(4).select_dtypes(include=np.number).sum()
    df['財報日期'] = 歷史財報.iloc[-1].財報日期
    df['財報類型'] = 歷史財報.iloc[-1].財報類型
    return df

@通知執行時間
@cache.memoize('取近年損益表', expire=15*24*60*60)
def 取近年損益表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報日期及財報類型。
    二、欄位：營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益
    二、預設傳回全部股票歷史合併或個別損益表，亦可傳回指定股票歷史損益表。
    三、來源為財報檔之損益表，並補正重大訊息公告財務報告摘要。
    四、排除逾一年未公布財報者。
    五、依累季前後期數據差異推算單季數據。
    六、亦能指定「個體報表」選項傳回個體損益表。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.現流表分析 import 取單期淨額
    import pandas as pd
    import numpy as np

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取近年損益表()
        return df.query('股票代號==@股票代號')

    df = 取損益表(個體報表=個體報表).query('頻率=="QE-DEC"')

    df = df.sort_values(by=["股票代號", "財報日期"])

    df = df.groupby(['股票代號'], as_index=False, group_keys=False).apply(取近年總額)
    df['股數'] = df.淨利/df.每股盈餘
    df['成本'] = df.營收 - df.毛利
    df['費用'] = df.毛利 - df.營利
    df['業外損益'] = df.稅前淨利 - df.營利
    df['其他損益'] = df.淨利- df.稅前淨利
    df['營收'] = df.營收.replace(0, np.nan) # 為不丢出除零例外，將 0 以 nan 取代，
    df['毛利率'] = df.毛利/df.營收
    df['營利率'] = df.營利/df.營收
    df['稅前淨利率'] = df.稅前淨利/df.營收
    df['淨利率'] = df.淨利/df.營收
    df['營利率'] = df.營利率.fillna(df.稅前淨利率)
    df['營收'] = df.營收.fillna(0)
    return df

@functools.cache
@通知執行時間
@cache.memoize('取年度損益表', expire=12*60*60)
def 取年度損益表(股票=None):
    '''
    一、即取第四季累積損益表。
    二、指定股票則傳回該股票年度損益表。
    三、主鍵為股票代號、財報日期及財報類型。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取年度損益表()
        return df.query('股票代號==@股票代號')
    df = 取累積損益表().query('財報日期.dt.month==12')
    return df
 
def 分析累積損益表結構(股票):
    from zhongwen.表 import 顯示
    import pandas as pd
    try:
        c = 最近累季損益表 = 取累積損益表(股票).iloc[-1]
    except IndexError:
        return pd.Series()
    評分結果 = pd.Series()
    if c.業外比重 > 0.3:
        評分結果[f'本期累積業外比重達{c.業外比重:,.2%}'] = -10
    return 評分結果

def 分析年度損益表結構(股票):
    from twse_crawler.股票基本資料分析 import 取股票基本資料彙總表
    from zhongwen.表 import 顯示
    import pandas as pd
    i = 取股票基本資料彙總表(股票).iloc[-1]
    if i.產業類別 == '金融保險業':
        評分結果 = 分析金融業年度損益表結構(股票)
    else:
        評分結果 = pd.Series()
    return 評分結果

def 分析金融業年度損益表結構(股票):
    import pandas as pd
    import numpy as np
    r = 最近完整年度損益表 = 取年度損益表(股票).iloc[-1]
    收益項目 = ['利息淨收益'
               ,'手續費淨收益'
               ,'手續費及佣金淨收益'
               ,'保險業務淨收益'
               ,'透過損益按公允價值衡量之金融資產及負債損益'
               ,'透過其他綜合損益按公允價值衡量之金融資產已實現損益'
               ,'採用權益法認列之關聯企業及合資損益之份額'
               ,'投資性不動產損益'
               ,'兌換損益'
               ,'投資淨損益'
               ]
    for i in 收益項目:
        r[f'{i}占淨收益比率'] = np.nan
    try:
        淨收益 = r['淨收益']
    except KeyError: pass

    r['投資淨損益'] = np.nan
    for i in ['透過損益按公允價值衡量之金融資產及負債損益'
             ,'透過其他綜合損益按公允價值衡量之金融資產已實現損益']:
        try:
            if pd.notna(d[i]):
                if pd.isna(d['投資淨損益']):
                    r['投資淨損益'] = r[i]
                else:
                    r['投資淨損益'] += r[i]
        except Exception as e: pass
    for i in 收益項目:
        try:
            r[f'{i}占淨收益比率'] = r[i] / 淨收益
        except Exception as e: pass

    分數, 評語 = 0, ''
    if pd.notna(r.手續費及佣金淨收益占淨收益比率) and r.手續費及佣金淨收益占淨收益比率>0.2:
        分數 += 100
        評語 = f'金融保險業務之手續費及佣金占淨收益比率達{r.手續費及佣金淨收益占淨收益比率:,.2%}'
    if pd.notna(r.手續費淨收益占淨收益比率) and r.手續費淨收益占淨收益比率>0.2:
        分數 += 100
        評語 = f'金融保險業務之手續費占淨收益比率達{r.手續費淨收益占淨收益比率:,.2%}'
    r = pd.Series()
    r['分數'] = 分數
    r['評語'] = 評語
    return r

def 分析損益表結構(股票):
    '對損益表各項目加以比較分析，亦稱為結構分析、比率分析，如毛利除營收之毛利率分析'
    import pandas as pd
    c = 損益表 = 取損益表(股票).iloc[-1]
    分數 = 0
    評語 = []
    if c.毛利率 < 0:
        分數 -= 100
        評語.append(f'產生毛損率{abs(c.毛利率):,.2%}')

    if c.營利率 < 0:
        分數 -= 300
        評語.append(f'產生營損率{abs(c.營利率):,.2%}')

    if c.毛利率 > 0.3:
        分數 += 500
        評語.append(f'毛利率高達{abs(c.毛利率):,.2%}')

    if c.營利率 > 0.2:
        分數 += 200
        評語.append(f'營利率高達{abs(c.營利率):,.2%}')
    r = pd.Series() 
    r['分數'] = 分數
    r['評語'] = '；'.join(評語)
    return r

def 取前年至次年各季損益表(股票):
    '''
    一、自指定「股票」歷季損益表之取出已有之前年至次年各季損益表。
    二、如快取日期落後最新財報日期時，即予更新。
    三、如無損益表資料，則引發數據不足錯誤。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱 
    from zhongwen.時 import 今日, 取正式民國日期
    from zhongwen.數 import 取最簡約數
    from zhongwen.文 import 臚列
    from zhongwen.表 import 數據不足, 顯示
    import pandas as pd
    import numpy as np
    歷季損益 = 取損益表(股票)
    if 歷季損益.empty:
        raise 數據不足(f'{股票}損益表', 0, 1, '取前年至次年各季損益表')
    歷季損益 = 歷季損益.drop_duplicates(subset='財報日期', keep='first')
    歷季損益 = 歷季損益.set_index('財報日期')
    歷季損益['營收'] = 歷季損益.營收.fillna(0)
    最近數據年度 = 歷季損益.iloc[-1].name.year
    df = 歷季損益
    df['損益期間季數'] = 1
    損益期間應含季數 = 4
    df = df.resample('YE-DEC').agg(
                     {'每股盈餘':sum, '損益期間季數':'size'})
    df = df.query('not 損益期間季數<@損益期間應含季數 or index.dt.year == @最近數據年度')
    del df['損益期間季數']
    歷季損益 = 歷季損益[歷季損益.index.year.isin(df.index.year)]
    最近一季損益 = 歷季損益.iloc[-1]
    本次資料日期 = 最近一季損益.name
    公司代號 = 最近一季損益.股票代號
    公司簡稱 = 查股票簡稱(公司代號)
    今年數 = 今日.year
    前年數 = 今年數-1
    次年數 = 今年數+1
    前年至次年月末 = pd.date_range(f'{前年數}0131', f'{次年數}1231', freq='QE')
    前年至次年季度損益 = 歷季損益.reindex(index=前年至次年月末)
    return 前年至次年季度損益[['股票代號', '財報類型'
     ,'資訊來源', '營收', '毛利', '營利', '業外損益', '稅前淨利', '淨利'
     ,'稀釋每股盈餘', '基本每股盈餘', '股數']]

def 分析損益表(股票):
    from twse_crawler.財報分析 import 彙總分析
    from twse_crawler.淨利趨勢分析 import 分析損益趨勢
    return 彙總分析(股票, [分析累積損益表結構
                          ,分析年度損益表結構
                          ,分析損益表結構
                          # ,分析損益趨勢
                          ])

def 分析營收(股票):
    '''
    一、分析結果：說明。
    二、分析季營收同比差異，並應進一步分析合約負債、台積電資本資出(未實作)、月營收、匯率等營收領先指標。
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    import pandas as pd
    歷季財報 = 取損益表(股票)
    if len(歷季財報) < 5: return pd.Series()
    msg = ''
    r1 = 分析歷季數據增減情形(歷季財報, '營收', '財報日期'
                             ,表達數據季別=False
                             ,表達本季數值=False
                             )
    if not r1.empty:
        msg += r1.說明
    else:
        msg += '主要係營收差異所致'
    r = pd.Series()
    r['說明'] = msg
    return r


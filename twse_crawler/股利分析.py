from zhongwen.pandas_tools import 可顯示, 製作排行榜
from zhongwen.快取 import 增加快取時序分析結果
from zhongwen.程式 import 通知執行時間
from zhongwen.庫 import 增加定期更新
from diskcache import Cache, Index
from pathlib import Path
import functools
import logging

DEBUG=False
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
logger = logging.getLogger(Path(__file__).stem)

歷史股利分析結果檔 = Index(str(Path.home() / '.twse_crawler' / r'資料庫/股利分派情形分析結果檔'))

@functools.cache
@通知執行時間
@cache.memoize('取股利表', expire=24*60*60)
def 取股利表(股票=None):
    '''
    一、主鍵：公司代號、股利所屬年度或股利所屬期間。
    二、欄位：公告日期、除息交易日、現金股利發放日、除權交易日、權利分派基準日、
              配息、配股
    三、傳回全部股票歷史股利表，亦可傳回指定股票歷史股利表。
    四、來源為公司股利分派公告資料彙總表。
    五、過濾股利為零或空值者，惟應了解前者為何會寫入資料庫，
        並避免類此情事發生，以提升空間及存取效率。
    六、股利分派情形彙總表係彙總董事會擬分派股利決議，尚無除權息日，作為股利表補充
    '''
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取公司股利分派公告資料彙總表
    from twse_crawler.股利分派情形爬蟲 import 抓取股利分派情形彙總表
    from twse_crawler.公開資訊觀測站爬蟲 import 股利資料庫
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.時 import 取期間, 今日, 本年度, 上年度
    from zhongwen.庫 import 批次載入
    from zhongwen.數 import 取數值
    from zhongwen.表 import 表示
    import pandas as pd

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取股利表()
        return df.query('公司代號==@股票代號')

    抓取公司股利分派公告資料彙總表(今日)

    df = 批次載入(股利資料庫, '股利表', '公告民國年度'
                 ,時間欄位=['公告日期'
                           ,'除息交易日', '現金股利發放日'
                           ,'除權交易日', '權利分派基準日'
                           ])
    df['配息'] = pd.to_numeric(df.盈餘配息
                              ,errors='coerce').fillna(0) + pd.to_numeric(
                                      df.公積配息, errors='coerce').fillna(0) 
    df['配股'] = pd.to_numeric(df.盈餘配股
                              ,errors='coerce').fillna(0) + pd.to_numeric(
                                      df.公積配股, errors='coerce').fillna(0)  
    df['原股利所屬期間'] = df.股利所屬期間

    def 取股利所屬期間(股利數據):
        import pandas as pd
        # 鈊象112年度股利期間誤植為112年後半年度，於程式補正
        if 股利數據.公司代號 == '3293':
            return 取期間(股利數據.股利所屬年度)
        elif pd.notna(股利數據.股利所屬期間):
            return 取期間(股利數據.股利所屬期間)
        else:
            return 取期間(股利數據.股利所屬年度)

    df['股利所屬期間'] = df.apply(取股利所屬期間, axis=1)
    df['股利所屬年度'] = df.股利所屬期間.map(lambda p: 取期間(p.year))

    #  抓取股利分派情形彙總表作為補充
    # if 今日.month < 4:
        # df1 = 抓取股利分派情形彙總表(上年度)
    # else:
    df1 = 抓取股利分派情形彙總表(本年度)
    df1 = df1[["公司代號", '股利所屬期間',"配息" , "配股", "股利所屬年度"]]
    df1['股利所屬期間'] = df1.apply(取股利所屬期間, axis=1)

    df['股利所屬期間表達'] = df.股利所屬期間.map(repr)
    df1['股利所屬期間表達'] = df1.股利所屬期間.map(repr)
    df = df.merge(df1, how="outer", on=["公司代號", "股利所屬期間表達"])

    df = df.rename(columns=lambda r: r.replace("_x", ""))
    df['配息'] = df.配息.fillna(df["配息_y"])
    df['配股'] = df.配股.fillna(df["配股_y"])
    df['股利所屬期間'] = df.股利所屬期間.fillna(df["股利所屬期間_y"])
    df['股利所屬年度'] = df.股利所屬年度.fillna(df["股利所屬年度_y"])
    for c in df.columns:
        if "_y" in c:
            del df[c]

    df['普通股每股面額'] = df.普通股每股面額.map(取數值)
    df = df.drop_duplicates(subset=['公司代號', '股利所屬期間', '配息', '配股'], keep='last')
    # 應過濾股利為零或空值者
    df = df.query('not ((配息==0 or 配息.isna()) '
                  'and (配股==0 or 配股.isna()) '
                  'and (特別股配息==0 or 特別股配息.isna()))')
    return df

def 取除權息概述(股票):
    '''
    如無除權息資料回傳空字串。
    '''
    from zhongwen.時 import 取民國日期, 今日
    from datetime import date
    import pandas as pd
    try:
        r = 取股利表(股票).query('公告日期.dt.year == @今日.year').iloc[-1]
    except IndexError:
        return ''
    m = ''
    if isinstance(r.配息, float) and r.配息 > 0:
        if pd.notna(r.除息交易日):
            待除息日數 = (r.除息交易日 - 今日).days
            if 待除息日數 < 0:
                m += f'已除息{r.配息:.2f}元'
            else:
                m += f'將於{待除息日數}日後{取民國日期(r.除息交易日, "%M月%d日")}發放股息{r.配息:.2f}元'
    if isinstance(r.配股, float) and r.配股 > 0:
        if pd.notna(r.除權交易日):
            待除權日數 = (r.除權交易日 - 今日).days
            if 待除權日數 < 0:
                m += f'已除權{r.配股:.2f}元'
            else:
                m += f'將於{待除權日數}日後{取民國日期(r.除權交易日, "%M月%d日")}配發股票{r.配股:.2f}元'
    return m

@functools.cache
@通知執行時間
@cache.memoize('取歷年股利表', expire=24*60*60)
def 取歷年股利表(股票=None):
    '''
    一、主鍵：公司代號、股利所屬年度。
    二、欄位：配息、配股、除息交易日、除權交易日及現金股利發放日。
    三、預設為所有股票股利表亦可取指定股票股利表。
    四、來源係公開資訊觀測站公司股利分派公告資料彙總表。
    五、已過濾股利為零或空值者，惟應了解前者為何會寫入資料庫，並避免類此情事發生，以提升空間及存取效率。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.時 import 取期間
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取歷年股利表()
        return df.query('公司代號==@股票代號')
    歷史股利 = 取股利表()
    歷年股利 = 歷史股利.groupby(['公司代號', '股利所屬年度']
                                ,as_index=False).agg({'配息':'sum'
                                                     ,'配股':'sum'
                                                     ,'除息交易日':'max'
                                                     ,'除權交易日':'max'
                                                     ,'現金股利發放日':'max'
                                                     })
    return 歷年股利

@functools.cache
@通知執行時間
@cache.memoize('取歷年每股盈餘表', expire=12*60*60)
def 取歷年每股盈餘表(股票=None):
    '''
    一、即取第四季累積損益表之每股盈餘。
    二、指定股票則傳回該股票歷年每股盈餘表。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from 股票分析.損益表分析 import 取年度損益表
    from zhongwen.表 import 顯示
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取年度損益表()
        return df.query('股票代號==@股票代號')

    df = 取年度損益表()
    df = df.query('財報類型.str.contains("合併|個別")')
    df = df[['股票代號', '財報日期', '每股盈餘']]
    df.set_index('財報日期', inplace=True)
    df['盈餘年度'] = df.index.to_period(freq='Y-DEC')
    if df.empty:
        raise Exception(f'尚無{查股票簡稱(股票)}損益表數據！')
    return df 

class 無配息率數據(Exception):pass
@通知執行時間
def 預測配息率(股票, 歷年股利=None):
    '''
    一、如有股利及損益表，據推算最近三次年度配息率，取其平均設算預測值，惟上限為150％；
        無則設算分配半數盈餘。
    二、預測結果資料：預測配息率、預測配息率說明。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    from zhongwen.時 import 取民國期間
    from zhongwen.表 import 顯示
    from zhongwen.文 import 臚列
    import pandas as pd
    import numpy as np
    預測結果 = pd.Series()
    預測配息率說明 = ''
    try:
        if not 歷年股利: 
            歷年股利 = 取歷年股利表(股票)
            最近股利 = 歷年股利.iloc[-1]
        公司代號 = 最近股利.公司代號
        公司簡稱 = 查股票簡稱(公司代號)
        h = 歷年每股盈餘 = 取歷年每股盈餘表(公司代號)
        h = h.set_index('財報日期')
        h['盈餘年度'] = h.index.to_period(freq='Y-DEC')
        歷年配息率 = h.merge(歷年股利, how='left'
                                     ,left_on='盈餘年度'
                                     ,right_on='股利所屬年度') 
        歷年配息率['股利'] = 歷年配息率.配息 + 歷年配息率.配股 
        歷年配息率['配息率'] = 歷年配息率.股利 / 歷年配息率.每股盈餘
        歷年有效配息率 = 歷年配息率.dropna(subset="配息率").query('配息率 > 0')
        歷年有效配息率 = 歷年配息率[歷年配息率.配息率.lt(np.inf) &
                                    歷年配息率.配息率.gt(-np.inf)
                                   ]
        近三年有效配息率 = 歷年有效配息率.tail(3)
        if not 近三年有效配息率.empty:
            # 運用近三年有效配息率平均預測，係避免如遠東銀112年度
            預測配息率 =  近三年有效配息率.配息率.mean()
            近三個有效配息年度 = [取民國期間(y) for y in 近三年有效配息率.股利所屬年度] 
            if 預測配息率 < 1.5:
                預測配息率說明 = \
                        f'以近三次({臚列(近三個有效配息年度)})平均配息率預測配息率為{預測配息率 :.2%}'
            else:
                預測配息率 = 1.5
                預測配息率說明 = \
                f'因近三次({臚列(近三個有效配息年度)})平均配息率逾預測上限150%，依上限設算配息率'
        else:
            預測配息率 = 預測結果['預測配息率'] = 0.5
            預測結果['預測配息率說明'] = '無有效配息率資料，爰設算分配半數盈餘'
    except IndexError as e:
        預測配息率 = 0.5
        預測配息率說明 = f'因無歷年股利資料，以預測配息率，爰預測分配半數盈餘'

    預測結果['預測配息率'] = 預測配息率
    預測結果['預測配息率說明'] = 預測配息率說明
    return 預測結果

def 依損益表預測每股盈餘(股票):
    '''
    一、運用指定「股票」歷季損益表預測前年至次年每股盈餘。
    二、僅公布一季損益表資料則將該季每股盈餘作為前年至次年每季每股盈餘預測。
    三、預測結果：1.前年至次年每股盈餘；2.預測說明。
    '''
    from 股票分析.損益表分析 import 取前年至次年各季損益表, 取損益表
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.自結損益 import 預測前年至次年周期數據
    from zhongwen.時 import 取正式民國日期
    from zhongwen.數 import 取最簡約數
    from zhongwen.表 import 顯示
    from zhongwen.文 import 臚列
    import pandas as pd
    預測結果 = pd.Series()
    r = 取前年至次年各季損益表(股票)
    if len(notnas:=r.dropna(subset=['基本每股盈餘'])) == 1:
        唯一數據 = notnas.iloc[0]
        簡稱 = 查股票簡稱(唯一數據.股票代號) 
        財報日期 = 取正式民國日期(唯一數據.name)
        預測說明 =  f'{簡稱}僅公布{財報日期}財報'
        預測說明 += '，將該季每股盈餘作為前年至次年各季每股盈餘預測'
        r['基本每股盈餘'] = r.基本每股盈餘.fillna(唯一數據.基本每股盈餘)
        年度每股盈餘 = r.基本每股盈餘.resample('YE').sum()
        年度每股盈餘對 = [(d.year, v) for d, v in 年度每股盈餘.items()] 
        年度 = [f'{t[0]-1911}' for t in 年度每股盈餘對] 
        每股盈餘 = [f'{t[1]:.2f}' for t in 年度每股盈餘對] 
        預測說明 += f'{臚列(年度)}年度每股盈餘為{臚列(每股盈餘)}元'
        預測結果['前年至次年每股盈餘'] = pd.Series(年度每股盈餘)
        預測結果['預測說明'] = 預測說明 
        return 預測結果
    else:
        h = 歷史季損益表 = 取損益表(股票)
        try:
            最近損益 = h.iloc[-1]
            前年至次年各季營利, 預測說明, *_ = 預測前年至次年周期數據(
                h, '營利', '財報期別', 取樣頻率='QE')

            前年至次年各季業外損益, *_ = 預測前年至次年周期數據(
                h, '業外損益', '財報期別', 取樣頻率='QE')
            預測說明 += f'、業外損益'

            前年至次年各季稅前損益 = 前年至次年各季營利 + 前年至次年各季業外損益
            預測說明 += f'、稅前損益'

            前年至次年各季淨利 = 前年至次年各季稅前損益*0.8
            預測說明 += f'及淨利'
            try:
                r = 取股票基本資料彙總表(股票)
                股數 = r['已發行普通股數或TDR原發行股數'].iloc[-1]
                前年至次年各季每股盈餘 = 前年至次年各季淨利/股數
                預測說明 += f'，再除以股票基本資料列流通股數{取最簡約數(股數)}股，'
            except Exception as e:
                logger.error(e)
                股數 = 最近損益.股數
                前年至次年各季每股盈餘 = 前年至次年各季淨利/股數
                預測說明 += f'，再除以{取正式民國日期(最近損益.name)}損益表推論流通股數'
                預測說明 += f'{取最簡約數(股數)}股，'

            年度每股盈餘 = 前年至次年各季每股盈餘.resample('YE').sum()
            年度每股盈餘對 = [(d.year, v) for d, v in 年度每股盈餘.items()] 
            年度 = [f'{t[0]-1911}' for t in 年度每股盈餘對] 
            每股盈餘 = [f'{t[1]:.2f}' for t in 年度每股盈餘對] 
            預測說明 += f'預測{臚列(年度)}年度每股盈餘分別為{臚列(每股盈餘)}元'
        except ValueError as e:
            前年至次年各季淨利, 預測說明, *_ = 預測前年至次年周期數據(
                h, '淨利', '財報期別', 取樣頻率='QE')

            股數 = 最近損益.股數
            前年至次年各季每股盈餘 = 前年至次年各季淨利/股數
            預測說明 += f'，再除以{取正式民國日期(最近損益.name)}損益表推論流通股數'
            預測說明 += f'{取最簡約數(股數)}股，'

            年度每股盈餘 = 前年至次年各季每股盈餘.resample('YE').sum()
            年度每股盈餘對 = [(d.year, v) for d, v in 年度每股盈餘.items()] 
            年度 = [f'{t[0]-1911}' for t in 年度每股盈餘對] 
            每股盈餘 = [f'{t[1]:.2f}' for t in 年度每股盈餘對] 
            預測說明 += f'預測{臚列(年度)}年度每股盈餘分別為{臚列(每股盈餘)}元'


        預測結果['前年至次年每股盈餘'] = 年度每股盈餘
        預測結果['預測說明'] = 預測說明
        return 預測結果

def 依歷年股利預測股利(股票):
    '''
    一、運用指定「股票」歷季損益表預測前年至次年每股盈餘。
    二、僅公布一季損益表資料則將該季每股盈餘作為前年至次年每季每股盈餘預測。
    三、預測結果：前年至次年股利、預測說明。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號
    from 股票分析.損益表分析 import 取前年至次年各季損益表, 取損益表, cache as acache
    from twse_crawler.自結損益 import 預測前年至次年周期數據
    from zhongwen.時 import 取正式民國日期
    from zhongwen.數 import 取最簡約數
    from zhongwen.表 import 數據不足, 顯示
    from zhongwen.文 import 臚列
    import pandas as pd
    acache.clear()
    try:
        h = 取歷年股利表(股票)
        n = 查股票簡稱(股票)
        h['股利'] = h.配息.fillna(0) + h.配股.fillna(0)
        r = 預測前年至次年周期數據(h, '股利', '股利所屬年度', 'YE')
        r = r.rename({'前年至次年各期數據':'前年至次年股利'})
        return r
    except 數據不足 as e:
        e.名稱 = f'{n}有效股利'
        raise e

@functools.cache
@通知執行時間
def 預測股利(股票, 歷年股利=None):
    '''
    一、如有自結損益，則據以預估前年至次年股利；
        無則循月營收、歷季損益表及歷年股利次序遞試之；
        悉闕，則告以數據不足。
    二、預測結果：前年至次年股利、預測股利說明、除息交易日、除權交易日、現金股利發放日
    三、除息交易日為最近一次除息交易日。
    四、如已公布上年度股利，則說明較預測增減情形。
    '''
    from twse_crawler.自結損益 import 預測前年至次年每股盈餘 as 依自結損益預測每股盈餘
    from twse_crawler.營收分析 import 預測前年至次年每股盈餘 as 依月營收預測每股盈餘
    from 股票分析.損益表分析 import 取損益表, 取前年至次年各季損益表
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號
    from zhongwen.快取 import 刪除指定名稱快取
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.文 import 臚列
    from zhongwen.數 import 取增減百分比
    import pandas as pd
    配息率, 配息率說明 = 預測配息率(股票)
    for 預測每股盈餘 in [依自結損益預測每股盈餘, 依月營收預測每股盈餘, 依損益表預測每股盈餘]:
        try:
            r = 預測每股盈餘(股票)
            前年至次年每股盈餘 = r.前年至次年每股盈餘
            預測股利說明 = r.預測說明
            前年至次年股利 = 前年至次年每股盈餘.clip(lower=0) * 配息率
            預測股利說明 = f'{預測股利說明}，乘{配息率說明}' 
            break
        except 數據不足 as e:
            continue
    else: # 依歷年股利預測股利
        前年至次年股利, 預測股利說明, *_ = 依歷年股利預測股利(股票) 
    年度股利對 = [(d.year, v) for d, v in 前年至次年股利.items()] 
    年度 = [f'{t[0]-1911}' for t in 年度股利對] 
    股利 = [f'{t[1]:.2f}' for t in 年度股利對] 
    預測股利說明 += f'，預測{臚列(年度)}年度股利分別為{臚列(股利)}元'
    預測股利說明 = 預測股利說明.replace('及0.00元', '及不配發股利', -1)
    預測結果 = pd.Series()
    try:
        d, m = 取上年度股利及說明(股票)
        delta_percent = (d - 前年至次年股利.iloc[0])/前年至次年股利.iloc[0]
        預測股利說明 += f'，{m}，較預測{取增減百分比(delta_percent)}'
        前年至次年股利.iloc[0] = d
    except Exception as e:
        logger.error(e)
    預測結果['前年至次年股利'] = 前年至次年股利
    預測結果['預測股利說明'] = 預測股利說明
    for d in ['除息交易日', '除權交易日', '現金股利發放日']:
        try:
            最近股利 = 取歷年股利表(股票).iloc[-1]
            預測結果[d] = 最近股利[d]
        except Exception:
            預測結果[d] = pd.NaT
    return 預測結果

# @增加快取最近時序分析結果(歷史股利分析結果檔, '公司代號'
                         # ,['股利所屬期間', '財報期別', '營收月份', '自結損益月份'], '股利')
def 分析歷史股利(歷年股利分派情形, 重新分析=False):
    '主鍵為公司代號'
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    from zhongwen.文 import 臚列
    from zhongwen.時 import 取日期
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    分析結果 = 歷年股利分派情形.iloc[-1]
    # 顯示(歷年股利分派情形)
    公司代號 = 分析結果.公司代號
    股票簡稱 = 查股票簡稱(公司代號)
    logger.info(f'分析{股票簡稱}股票歷年股利分派情形！')
    try:
        分析結果 = pd.concat([分析結果, 預測配息率(歷年股利分派情形)])
    except (無損益表數據, 無配息率數據, IndexError) as e:
        logger.error(str(e))
        分析結果['前年至次年各年股利'] = None
        分析結果['預測股利說明'] = f'{e}'
        return 分析結果

    try:
        前年至次年各年股利 = (分析結果.前年至次年各年每股盈餘.clip(lower=0) * 
                              min(分析結果.預測配息率, 1.5))
    except AttributeError:
        logger.error(f'{股票簡稱}無法預測股利')
        分析結果['前年至次年各年股利'] = pd.Series()
        分析結果['預測股利說明'] = '無法預測股利'
        return 分析結果
 
    df = 歷年股利分派情形[['股利所屬年度', '配息', '配股']]
    df['股利'] = df.配息 + df.配股
    df = df.groupby('股利所屬年度').股利.sum()
    df.index = df.index.map(lambda y: 取日期(f'{y}1231'))
    # df = df.股利
    # print(前年至次年各年股利.index)
    df = df.reindex(前年至次年各年股利.index)
    df = df.fillna(前年至次年各年股利)
    前年至次年各年股利 = df
    # 顯示(df, 顯示索引=True)

    預測股利說明 = f'{分析結果.預測每股盈餘說明}，乘{分析結果.預測配息率說明}' 
    年度股利對 = [(d.year, v) for d, v in 前年至次年各年股利.items()] 
    年度 = [f'{t[0]-1911}' for t in 年度股利對] 
    股利 = [f'{t[1]:.2f}' for t in 年度股利對] 
    預測股利說明 += f'，預測{臚列(年度)}年度股利分別為{臚列(股利)}元'
    分析結果['前年至次年各年股利'] = 前年至次年各年股利
    分析結果['預測股利說明'] = 預測股利說明.replace('0.00元', '不配發股利', -1)
    return 分析結果

@cache.memoize('年度股利分析表', expire=24*60*60)
def 年度股利分派表(股票代號組=[]):
    '按股利所屬年度彙計配息及配股，索引為公司代號及股利所屬年度'
    from twse_crawler.公開資訊觀測站爬蟲 import 載入股利表
    from zhongwen.date import 今日, 上年初
    from collections.abc import Iterable
    import pandas as pd
    df = 載入股利表()
    if 股票代號組:
        if not isinstance(股票代號組, Iterable):
            股票代號組 = [股票代號組]
        df = df.query('公司代號 in @股票代號組')
    df = df.reset_index()
    df = df[['公司代號', '股利所屬年度', '配息', '配股', '公告日期']]
    df = df.drop_duplicates()
    df = df.groupby(['公司代號', '股利所屬年度'])[['配息', '配股']].sum().sort_index()
    # 今日至年底期間仍有可以配發股利，爰排除本年度。
    df = df.query('股利所屬年度 < @今日().year-1911') 
    return df

def 取上年度股利及說明(股票):
    '''
    一、傳回指定股票上年度股利及說明。
    '''
    from zhongwen.時 import 上年度
    from zhongwen.表 import 表示
    df = 取股利表(股票)
    r = df.query('股利所屬年度==@上年度').iloc[-1]
    m = '、'.join(f'{d}{r[d]:.2f}元' for d in ['配息', '配股'] if r[d] > 0)
    if r.配息>0 and r.配股 > 0:
        m = f'{上年度.year-1911}年實際{m}，合計{r.配息+r.配股:.2f}元'
    else:
        m = f'{上年度.year-1911}年實際{m}'
    return r.配息+r.配股, m

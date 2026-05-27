from diskcache import Cache
from pathlib import Path
from zhongwen.庫 import 結果批次寫入
from zhongwen.程式 import 通知執行時間
from zhongwen.時 import 今年數
import functools
import logging

logging.getLogger("prophet").setLevel(logging.CRITICAL)
logging.getLogger("cmdstanpy").setLevel(logging.CRITICAL)

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
鉛價庫 = Path.home() / '.twse_crawler' / '資料庫' / '鉛價庫'

毛利受鉛價影響者 = ['泰銘']

@結果批次寫入(鉛價庫, '鉛價', '年度數', list(range(2008, 今年數+1)))
def 抓取年度鉛價(年度數):
    '''
    一、LME 交易價。
    二、批號為年度數。
    三、資料來源：https://www.westmetall.com/en/markdaten.php。
    '''
    from zhongwen.數 import 取數值
    from dateutil import parser
    import pandas as pd
    import time 
    logger.info(f'爬取{年度數}年度鉛價……')
    url = 'https://www.westmetall.com/en/markdaten.php'
    url += f'?action=table&field=LME_Pb_cash&year={年度數}'
    df = pd.read_html(url)[0]
    df = df.query('not date.str.contains("date")')
    df['date'] = df.date.map(parser.parse)
    df = df.replace('-', '0')
    df = df.astype({'LME Lead Cash-Settlement': 'float'
                   ,'LME Lead 3-month': 'float'
                   ,'LME Lead stock': 'int'
                   })
    df.columns = ['日期', '現價', '三月期貨價', '庫存']
    df['年度數'] = 年度數
    time.sleep(30)
    logger.info(f'完成！')
    return df

@cache.memoize('取鉛價', expire=24*60*60)
def 取鉛價():
    '''
    一、索引為 pd.DatetimeIndex。
    二、欄位：現價、三月期貨價、庫存。
    三、每日更新。
    '''
    from zhongwen.庫 import 批次載入
    from zhongwen.時 import 昨日
    df = 批次載入(鉛價庫, '鉛價', '年度數', 時間欄位='日期', 起始批號=2008).sort_values('日期')
    df = df.set_index('日期')
    最近日期 = df.index.max()
    if 最近日期 < 昨日:
        抓取年度鉛價(昨日.year)
        df = 批次載入(鉛價庫, '鉛價', '年度數', 時間欄位='日期', 起始批號=2008).sort_values('日期')
        df = df.set_index('日期')
    return df

@通知執行時間
@cache.memoize('預測次年底鉛價', expire=24*60*60)
def 預測次年底鉛價():
    '''
    一、預估結果項目：預估值、預估季均值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                      模型名稱、模型參數、最近歷史值日期、最後預估值日期、
                      預估方法說明、預估說明。
    二、預估季均值：季均值、季起日值、季迄日值、季增減數。
    '''
    from twse_crawler.預估次年底 import 預估次年底值, 表達預估方法
    r = 預估次年底值(取鉛價().現價)
    r['預估方法'] = 表達預估方法(r, 時間單位='日') 
    return r

def 預測次年底營收(股票):
    """
    一、傳回預估每月值、預估每季總值、預估方法說明及預估說明。
    """
    from twse_crawler.預估次年底 import 預估至次年底每月值
    from twse_crawler.營收分析 import 取歷月營收表
    from twse_crawler.預估次年底 import 表達預估方法
    import pandas as pd
    df = 取歷月營收表(股票).set_index('營收月份').營收
    df.index = df.index.asfreq('M')
    p = 預估至次年底每月值(df)
    p['預估方法說明'] = 表達預估方法(p, '營收', 時間單位='月')
    p['預估說明'] = 表達預估說明(p, '營收')
    return p

@通知執行時間
@cache.memoize('以鉛價預測次年底毛利率', expire=24*60*60)
def 以鉛價預測次年底毛利率(股票):
    """
    一、傳回預估季值、預估方法說明、預估說明。
    二、預估季值為歷史加各季預估值。
    """
    from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    from twse_crawler.預估次年底 import 表達預估方法
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.數 import 取增減百分比
    from zhongwen.時 import 取民國季度
    from zhongwen.表 import 表示
    預估鉛價結果 = 預測次年底鉛價()
    預估鉛價 = 預估鉛價結果.預估季均值[['季均值', '季增減數']]
    df = 取財報彙總表(股票).set_index('財報日期').to_period('Q')
    r = 依外部季數據預估次年底數值(df.毛利率, 預估鉛價)
    預估毛利率方法 = 表達預估方法(r)
    r['預估方法說明'] = (f'以{預估鉛價結果.預估方法}模型預測之鉛價'
                           f'，輸入以{預估毛利率方法}模型預測毛利率'
                          )
    預測首季度 = r.最近歷史值時間 + 1
    預測首季鉛價 = 預估鉛價結果.預估季均值.loc[預測首季度] 
    預測首季鉛價增減率 = 預測首季鉛價.季增減數 / 預測首季鉛價.季起日值
    r['預估說明'] = (
        f'{取民國季度(預測首季度)}鉛價{取增減百分比(預測首季鉛價增減率)}'
        f'，{表達預估說明(r, '毛利率')}' 
            )
    return r

def 表達預估說明(預估結果, 預估目標='毛利率'):
    from zhongwen.數 import 取增減百分比
    from twse_crawler.預估次年底 import 表達期間
    return f'{表達期間(預估結果.最近歷史值時間)}{預估目標}同比{取增減百分比(預估結果.最近歷史值同比)}，次季預估{取增減百分比(預估結果.首期預估值同比)}'

@通知執行時間
def 以鉛價預測次年每股盈餘(股票, 歷月營收表=None):
    '''
    一、運用指定「股票」歷月營收表預測前年至次年每股盈餘。
    二、預測結果：前年至次年每股盈餘、預估說明、預估方法說明
    三、相上相容：預測說明、營收趨勢起日、營收數據頻率、
        營收趨勢方向、營收趨勢斜率、營收趨勢說明、
    四、股票須公布2個月以上營收，否則產生「數據不足」例外。
    五、以股票快取預測結果，如快取日期落後最新營收日期時更新。
    六、增加預測上季每股盈餘及實際每股盈餘差異。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.自結損益 import 預測前年至次年周期數據
    from twse_crawler.營收分析 import 取預測盈餘說明
    from zhongwen.時 import 今年數, 取正式民國日期
    from zhongwen.快取 import 刪除指定名稱快取
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取最簡約數
    from zhongwen.文 import 臚列
    from zhongwen.表 import 表示
    import pandas as pd
    公司代號 = 查股票代號(股票)
    公司簡稱 = 查股票簡稱(股票)
    歷季損益表 = 取損益表(公司代號)
    歷季損益表['營收'] = 歷季損益表.營收.fillna(0)
    try:
        最近損益 = 歷季損益表.iloc[-1]
    except IndexError as e:
        raise 數據不足(f'{公司簡稱}歷季損益', 0, 1, '預測前年至次年每股盈餘')
    except Exception as e:
        errmsg = f'{type(e).__name__}({e})'
        logger.error(errmsg)
        logger.error(公司代號)
        raise Exception(f"{公司代號}{errmsg}")

    try:
        歷季損益表 = 歷季損益表.set_index(歷季損益表.財報日期.dt.to_period('Q'))
    except AttributeError:
        歷季損益表['財報日期'] = 歷季損益表.index
    預測營收結果 = 預測次年底營收(股票)
    預測營收 = 預測營收結果.預估每季總值.預估每季營收
    future_index = 預測營收.index[預測營收.index > 歷季損益表.index.max()]
    new_index = 歷季損益表.index.append(future_index)
    歷季損益表 = 歷季損益表.reindex(new_index)
    歷季損益表['營收'] = 歷季損益表.營收.fillna(預測營收)
    預測毛利率結果 = 以鉛價預測次年底毛利率(股票)
    預測毛利率 = 預測毛利率結果.預估各季值
    歷季損益表['毛利率'] = 歷季損益表.毛利率.fillna(預測毛利率)
    歷季損益表['毛利'] = 歷季損益表.毛利.fillna(預測營收*預測毛利率)
    from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    預測營利結果 = 依外部季數據預估次年底數值(歷季損益表.營利.dropna()
                                             ,歷季損益表[['毛利']]
                                             ,預估目標='營利', 單位='元')
    from twse_crawler.預估次年底 import 表達預估方法
    預測營利方法說明 = 表達預估方法(預測營利結果,'營利')
    預測營利 = 預測營利結果.預估各季值
    歷季損益表['營利'] = 歷季損益表.營利.fillna(預測營利)
    from twse_crawler.預估次年底 import 預估至次年底每季值
    # 預測業外損益結果 = 預估至次年底每季值(歷季損益表.業外損益.dropna())
    from twse_crawler.匯率分析 import 以匯率預測次年底業外損益, cache as cacheb
    cacheb.clear()
    預測業外損益結果 = 以匯率預測次年底業外損益(股票)
    預測業外損益 = 預測業外損益結果.預估各季值
    預測業外損益說明 = 表達預估說明(預測業外損益結果, '業外損益')
    歷季損益表['業外損益'] = 歷季損益表.業外損益.fillna(預測業外損益)
    歷季損益表['稅前淨利'] = 歷季損益表.稅前淨利.fillna(預測營利+預測業外損益)

    歷季損益表['淨利'] = 歷季損益表.淨利.fillna(歷季損益表.稅前淨利*0.8)
    q = 取股票基本資料彙總表(股票)
    股數 = q.股數.iloc[-1]
    歷季損益表['每股盈餘'] = 歷季損益表.每股盈餘.fillna(歷季損益表.淨利/股數)
    from zhongwen.時 import 前年至次年各季末
    前年至次年各季數據 = 歷季損益表.reindex(index=前年至次年各季末.to_period('Q'))
    年度每股盈餘 = 前年至次年各季數據.每股盈餘.resample('Y').sum()
    from twse_crawler.預估次年底 import 表達預測準確度
    預估方法說明 = (f'以{預測營收結果.預估方法說明}'
         f'，乘上{預測毛利率結果.預估方法說明}之毛利'
         f'，輸入以{預測營利方法說明}'
         f'，與以{預測業外損益結果.預估方法說明}'
         f'，加總之稅前損益'
         f'，扣除最高稅率20％之營所稅之損益'
         f'，再除以{取最簡約數(股數)}股之每股盈餘'
         )
    預估說明 = (f'{預測營收結果.預估說明}'
                f'，{預測毛利率結果.預估說明}'
                f'，{預測業外損益說明}'
                f'，{取預測盈餘說明(年度每股盈餘, 前年至次年各季數據)}'
        )
    預測結果 = pd.Series({'前年至次年每股盈餘': pd.Series(年度每股盈餘)
                         ,'預測說明':預估說明
                         ,'預估說明':預估說明
                         ,'預估方法說明':預估方法說明
                         })
    return 預測結果

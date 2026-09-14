from diskcache import Cache
from pathlib import Path
from zhongwen.庫 import 結果批次寫入
from zhongwen.程式 import 通知執行時間
from zhongwen.時 import 今年數
import functools
import logging

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
def 預測次年底鉛價(回滾日數=0):
    '''
    一、預估結果項目：預估每日值、預估季均值、
                      模型名稱、誤差率、歷史值數量、預估值數量、回滾資料數、
                      最佳訓練資料數、趨勢、近期影響權重
                      最近歷史值時間、最後預估值時間、
                      最近歷史值同比、首期預估值同比。
    二、預估季均值：季均值、季起日值、季迄日值、季增減數。
    '''
    from twse_crawler.預估至次年底每日值 import 預估至次年底工作日值丙式
    import pandas as pd
    ps = 取鉛價().現價
    d = ps.index.max()
    d -= pd.Timedelta(days=回滾日數)
    ps = ps[ps.index <= d]
    r = 預估至次年底工作日值丙式(ps)
    return r

@通知執行時間
def 以鉛價預測次年底毛利率(股票, 回滾季數=0):
    """
    一、預估結果項目：預估各季值、
                      模型名稱、誤差率、歷史值數量、預估值數量、回滾資料數、
                      最佳訓練資料數、趨勢、近期影響權重、自變數影響權重
                      最近歷史值時間、最後預估值時間、
                      最近歷史值同比、首期預估值同比、鉛價預測結果。
    二、預估季值為歷史加各季預估值。
    """
    from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.數 import 取增減百分比
    from zhongwen.時 import 取民國季度
    from zhongwen.表 import 表示
    預估鉛價結果 = 預測次年底鉛價(回滾日數=回滾季數*90)
    預估鉛價 = 預估鉛價結果.預估季均值[['季均值', '季增減數']]
    df = 取財報彙總表(股票).set_index('財報日期').to_period('Q')
    回滾季度 =  df.index.max()
    回滾季度 -= 回滾季數
    df = df[df.index <= 回滾季度]
    最近毛利率季度 = df.index.max()
    歷季毛利率對應鉛價 = 預估鉛價[預估鉛價.index <= 最近毛利率季度]
    預估毛利率對應鉛價 = 預估鉛價[預估鉛價.index > 最近毛利率季度]
    r = 以單元迴歸預估至次年底每季值(歷季毛利率對應鉛價, df.毛利率, 預估毛利率對應鉛價)
    預估毛利率方法 = 表達預估方法丙(r, '毛利率', '%')
    r['鉛價預測結果'] = 預估鉛價結果
    return r

@通知執行時間
def 以鉛價預測次年淨利(股票, 回滾季數=0, 估算模型誤差率=True):
    '''
    一、預估結果項目：預估各季值、
                      模型名稱、誤差率、歷史值數量、預估值數量、回測資料數、
                      最佳訓練資料數、趨勢、自變數影響權重
                      最近歷史值時間、最後預估值時間、
                      最近歷史值同比、首期預估值同比。
    二 、回滾季數係將指定股票目前資料往回季數之資料作為預測歷史資料，
        最近回滾資料作為評估誤差率使用。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    from twse_crawler.營收分析 import 取預測盈餘說明
    from zhongwen.快取 import 刪除指定名稱快取
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.表 import 表示, 數據不足
    from zhongwen.數 import 取最簡約數
    from zhongwen.文 import 臚列
    import pandas as pd
    公司代號 = 查股票代號(股票)
    公司簡稱 = 查股票簡稱(股票)
    if 公司簡稱 not in 毛利受鉛價影響者:
        from twse_crawler.股利分析 import 無法預估盈餘
        raise 無法預估盈餘(f'鉛價不影響{股票}之毛利率！')

    歷季損益表 = 取財報彙總表(公司簡稱)
    if 估算模型誤差率:
        from twse_crawler.無腦預測至次年底每季值 import calc_wmape
        y_true = 歷季損益表.淨利.iloc[-4:]
        y_pred = 以鉛價預測次年淨利(股票, 回滾季數=4, 估算模型誤差率=False).預估各季值
        y_pred = y_pred.loc[y_true.index]
        模型誤差率 = calc_wmape(y_true, y_pred)

    歷季損益表['營收'] = 歷季損益表.營收.fillna(0)
    回滾季度 = 歷季損益表.財報季度.max()
    回滾季度 -= 回滾季數
    歷季損益表 = 歷季損益表.query('財報季度 <= @回滾季度')
    歷史值數量 = 歷季損益表.shape[0]
    try:
        最近損益 = 歷季損益表.iloc[-1]
    except IndexError as e:
        raise 數據不足(f'{公司簡稱}歷季損益', 0, 1, '預測前年至次年每股盈餘')
    except Exception as e:
        errmsg = f'{type(e).__name__}({e})'
        m = f"{公司代號}發生{errmsg}"
        raise Exception(m)

    try:
        歷季損益表 = 歷季損益表.set_index(歷季損益表.財報日期.dt.to_period('Q'))
    except AttributeError:
        歷季損益表['財報日期'] = 歷季損益表.index

    # 計算業外影響程度 
    業外影響程度 = abs(歷季損益表.業外損益.iloc[-1]) / abs(歷季損益表.稅前淨利.iloc[-1])

    # 預測營收
    from twse_crawler.營收分析 import 預測次年底營收
    預測營收結果 = 預測次年底營收(股票, 回滾月數=回滾季數*3)
    預測營收 = 預測營收結果.預估每季總值
    最近財報季 = 歷季損益表.index.max()
    future_index = 預測營收.index[預測營收.index > 歷季損益表.index.max()]
    new_index = 歷季損益表.index.append(future_index)
    歷季損益表 = 歷季損益表.reindex(new_index)
    歷季損益表['營收'] = 歷季損益表.營收.fillna(預測營收)

    # 預測毛利率及毛利
    預測毛利率結果 = 以鉛價預測次年底毛利率(股票, 回滾季數=回滾季數)
    鉛價預測結果 = 預測毛利率結果.鉛價預測結果
    預測毛利率 = 預測毛利率結果.預估各季值
    歷季損益表['毛利率'] = 歷季損益表.毛利率.fillna(預測毛利率)
    歷季損益表['毛利'] = 歷季損益表.毛利.fillna(預測營收*預測毛利率)

    from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值
    歷季毛利 = 歷季損益表.毛利[歷季損益表.index <= 最近財報季]
    未來毛利 = 歷季損益表.毛利[歷季損益表.index > 最近財報季]
    預測營利結果 = 以單元迴歸預估至次年底每季值(歷季毛利
                                               ,歷季損益表.營利.dropna()
                                               ,未來毛利)
    預測營利方法說明 = 表達預估方法丙(預測營利結果,'營利')
    預測營利 = 預測營利結果.預估各季值
    歷季損益表['營利'] = 歷季損益表.營利.fillna(預測營利)

    # 預測業外損益
    from twse_crawler.預估次年底 import 預估至次年底每季值
    # 預測業外損益結果 = 預估至次年底每季值(歷季損益表.業外損益.dropna())
    from twse_crawler.匯率分析 import 以匯率預測次年底業外損益, cache as cacheb

    預測業外損益結果 = 以匯率預測次年底業外損益(股票, 回滾季數=回滾季數)
    預測業外損益 = 預測業外損益結果.預估各季值
    歷季損益表['業外損益'] = 歷季損益表.業外損益.fillna(預測業外損益)

    # 預測稅前淨利
    歷季損益表['稅前淨利'] = 歷季損益表.稅前淨利.fillna(預測營利+預測業外損益)

    # 預測淨利
    歷季損益表['淨利'] = 歷季損益表.淨利.fillna(歷季損益表.稅前淨利*0.8)
    預估損益表 = 歷季損益表.loc[future_index]

    y = 歷季損益表.loc[歷季損益表.index <= 最近財報季].淨利
    最近歷史值同比 = (
        (y.iloc[-1] - y.iloc[-5]) / y.iloc[-5]
        if len(y) > 4 and y.iloc[-5] != 0 else np.nan
    )

    預估季_序列 = 歷季損益表.淨利

    首期預估值同比 = (
        (預估季_序列.iloc[0] - y.iloc[-4]) / y.iloc[-4]
        if len(y) >= 3 and y.iloc[-4] != 0 else np.nan
    )

    最近季度 = y.index[-1]
    最後預估值時間 = 預估季_序列.index[-1]

    # 表達預估方法及預估結果
    from twse_crawler.預估次年底 import 移除重覆時間詞
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    try:
        預估方法說明 = (f'{表達預估方法丙(預測營收結果)}乘上'
             f'，{表達預估方法丙(預測毛利率結果)}，'
             f'，所得之毛利輸入以{預測營利方法說明}'
             f'，與以{表達預估方法丙(預測業外損益結果)}'
             f'，加總之稅前損益'
             f'，扣除最高稅率20％之營所稅之損益'
             )
    except:
        表示(預測營收結果)
        raise Exception('預估方法說明錯誤')
    from twse_crawler.預估至次年底每季值 import 表達預估說明丙
    預測結果 = pd.Series({"預估各季值": 預估損益表.淨利,
                          "模型名稱": '以鉛價預測次年淨利',
                          "誤差率": 模型誤差率 if 估算模型誤差率 else None,
                          "歷史值數量": 預測業外損益結果.歷史值數量,
                          "預估值數量": 預測業外損益結果.預估值數量,
                          "回測資料數": 4,
                          "最近歷史值時間": 預測業外損益結果.最近歷史值時間,
                          "最後預估值時間": 預測業外損益結果.最後預估值時間,
                          "最近歷史值同比": 最近歷史值同比,
                          "首期預估值同比": 首期預估值同比
                        })
    預估說明 = (f'【營收預測】{表達預估說明丙(預測營收結果, '營收', '月')}'
                f'【鉛價預測結果】{表達預估說明丙(鉛價預測結果, '鉛價', '日')}'
                f'【毛利率預測】{表達預估說明丙(預測毛利率結果, 預估目標='毛利率', 自變數名稱='鉛價')}'
                f'【營利預測】{表達預估說明丙(預測營利結果, 預估目標='營利', 自變數名稱='毛利')}'
                f'【匯率預測結果】{表達預估說明丙(預測業外損益結果.預估匯率結果, '匯率', '日')}'
                f'【業外損益預測】{表達預估說明丙(預測業外損益結果, 預估目標='業外損益', 自變數名稱='匯率')}'
                f'【淨利預測】{表達預估說明丙(預測結果, 預估目標='淨利')}'
                )
    預測結果['各項預估說明'] = 預估說明
    return 預測結果

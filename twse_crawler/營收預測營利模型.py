import numpy as np
import optuna
import pandas as pd
import statsmodels.api as sm
from twse_crawler.預估次年底 import 表達期間
from zhongwen.程式 import 通知執行時間
from twse_crawler.蒐整財務資訊 import 增加股票分析函數依資料時間更新快取功能
from diskcache import Cache, Index
from pathlib import Path
import logging

營收預測營利結果快取檔 = Index(str(Path.home() / '.twse_crawler' / '快取' / '營收預測營利結果快取檔'))
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
logger = logging.getLogger(Path(__file__).stem)

def 以營收預測次年營利(股票, 回滾季數=0):
    """
    一、比較以營收預測次年營利方式，及時間序列方式預測次年營利之誤差較小者。
    二、預估結果項目：預估各季值、
                      模型名稱、誤差率、歷史值數量、預估值數量、回滾資料數、
                      最佳訓練資料數、趨勢、近期影響權重、自變數影響權重
                      最近歷史值時間、最後預估值時間、
                      最近歷史值同比、首期預估值同比、預測營收結果。
    三、預估各季值為歷史加各季預估值。
    """
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.表 import 表示

    公司簡稱 = 查股票簡稱(股票)

    歷季損益表 = 取財報彙總表(公司簡稱)

    回滾季度 =  歷季損益表.index.max()
    回滾季度 -= 回滾季數

    # 預測營收
    from twse_crawler.營收分析 import 預測次年底營收
    預測營收結果 = 預測次年底營收(股票, 回滾月數=回滾季數*3)
    預測營收 = 預測營收結果.預估每季總值

    df = 歷季損益表[歷季損益表.index <= 回滾季度]
    最近營利季度 = df.index.max()
    預測營收 = 預測營收[預測營收.index > 最近營利季度]
    r = 以單元迴歸預估至次年底每季值(df.營收, df.營利, 預測營收)
    r['預測營收結果'] = 預測營收結果
    return r

@通知執行時間
def 以營收預測次年淨利(股票, 回滾季數=0, 估算模型誤差率=True):
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

    歷季損益表 = 取財報彙總表(公司簡稱)
    if 估算模型誤差率:
        from twse_crawler.無腦預測至次年底每季值 import calc_wmape
        y_true = 歷季損益表.淨利.iloc[-4:]
        y_pred = 以營收預測次年淨利(股票, 回滾季數=4, 估算模型誤差率=False).預估各季值
        # 表示(以營收預測次年淨利(股票, 回滾季數=4, 估算模型誤差率=False))
        y_pred = y_pred.loc[y_true.index]
        模型誤差率 = calc_wmape(y_true, y_pred)
        from twse_crawler.預估至次年底每季值 import 預估至次年底每季值丙式
        淨利預測結果 = 預估至次年底每季值丙式(歷季損益表.淨利)
        if 淨利預測結果.誤差率 < 模型誤差率:
            return 淨利預測結果
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

    預測營利結果 = 以營收預測次年營利(股票, 回滾季數)
    預測營利方法說明 = 表達預估方法丙(預測營利結果,'營利')
    預測營利 = 預測營利結果.預估各季值

    全時損益表 = 歷季損益表.copy()
    future_index = 預測營利.index[預測營利.index > 歷季損益表.index.max()]
    全時損益表 = 全時損益表.reindex(全時損益表.index.append(future_index))
    # 表示(全時損益表, 顯示索引=True)
    全時損益表['營利'] = 全時損益表.營利.fillna(預測營利)
    # 預測業外損益
    from twse_crawler.預估次年底 import 預估至次年底每季值
    from twse_crawler.匯率分析 import 以匯率預測次年底業外損益, cache as cacheb

    預測業外損益結果 = 以匯率預測次年底業外損益(股票, 回滾季數=回滾季數)
    預測業外損益 = 預測業外損益結果.預估各季值
    全時損益表['業外損益'] = 全時損益表.業外損益.fillna(預測業外損益)

    # 預測稅前淨利
    全時損益表['稅前淨利'] = 全時損益表.稅前淨利.fillna(預測營利+預測業外損益)

    # 預測淨利
    全時損益表['淨利'] = 全時損益表.淨利.fillna(全時損益表.稅前淨利*0.8)
    預估損益表 = 全時損益表.loc[future_index]
    最近財報季 = 歷季損益表.財報季度.max()
    y = 全時損益表.loc[全時損益表.index <= 最近財報季].淨利
    最近歷史值同比 = (
        (y.iloc[-1] - y.iloc[-5]) / y.iloc[-5]
        if len(y) > 4 and y.iloc[-5] != 0 else np.nan
    )
    預估季_序列 = 全時損益表.淨利
    首期預估值同比 = (
        (預估季_序列.iloc[0] - y.iloc[-4]) / y.iloc[-4]
        if len(y) >= 3 and y.iloc[-4] != 0 else np.nan
    )
    最近季度 = y.index[-1]
    最後預估值時間 = 預估季_序列.index[-1]
    # 表達預估方法及預估結果
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    try:
        預估方法說明 = (f'{表達預估方法丙(預測營利結果)}'
             f'，{表達預估方法丙(預測業外損益結果)}'
             f'，加總之稅前損益'
             f'，扣除最高稅率20％之營所稅之損益'
             )
    except Exception as e:
        表示(預測營利結果)
        raise Exception(f'預估方法說明發生：{e}')

    from twse_crawler.預估至次年底每季值 import 表達預估說明丙
    預測結果 = pd.Series({"預估各季值": 預估損益表.淨利,
                         "模型名稱": '以營收預測次年淨利',
                         "誤差率": 模型誤差率 if 估算模型誤差率 else None,
                         "歷史值數量": 預測業外損益結果.歷史值數量,
                         "預估值數量": 預測業外損益結果.預估值數量,
                         "回測資料數": 4,
                         "最近歷史值時間": 預測業外損益結果.最近歷史值時間,
                         "最後預估值時間": 預測業外損益結果.最後預估值時間,
                         "最近歷史值同比": 最近歷史值同比,
                         "首期預估值同比": 首期預估值同比
                        })
    預估說明 = (
                f'【營收預測】{表達預估說明丙(預測營利結果.預測營收結果, 預估目標='營收')}'
                f'【營利預測】{表達預估說明丙(預測營利結果, 預估目標='營利', 自變數名稱='營收')}'
                f'【匯率預測結果】{表達預估說明丙(預測業外損益結果.預估匯率結果, '匯率', '日')}'
                f'【業外損益預測】{表達預估說明丙(預測業外損益結果, 預估目標='業外損益', 自變數名稱='匯率')}'
                f'【淨利預測】{表達預估說明丙(預測結果, 預估目標='淨利')}'
                )
    預測結果['各項預估說明'] = 預估說明
    return 預測結果

from twse_crawler.蒐整財務資訊 import 增加股票分析函數依資料時間更新快取功能
from twse_crawler.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表, 自結損益資料庫
from fhopecc.洄瀾打狗人札記 import 可發布, 錢商品錢識別碼
from zhongwen.快取 import 增加快取時序分析結果
from zhongwen.庫 import 增加定期更新, 通知執行時間
from pathlib import Path
from diskcache import Cache, Index
import functools
import logging

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

自結損益分析結果快取檔 = Index(str(
    Path.home() / '.twse_crawler' / r'快取/自結損益分析結果快取檔'
    ))

歷次自結損益分析結果快取檔 = Index(str(
    Path.home() / '.twse_crawler' / r'資料庫/歷次自結損益分析結果快取檔'
    ))
歷史月稅前損益分析結果快取檔 = Index(str(
    Path.home() / '.twse_crawler' / r'資料庫/歷史月稅前損益分析結果快取檔'
    ))

logger = logging.getLogger(Path(__file__).stem)

def 預測前年至次年周期數據(歷史數據, 數據欄名
                          ,取樣時間欄名, 取樣頻率='ME'
                          ,輔助數據欄名=None
                          ,預測前年至次年輔助數據=None
                          ,數據名稱=None
                          ,顯示趨勢圖=False
                          ,不顯示數據名稱=False
                          ,季節性模式='multiplicative'
                          ,變化點數=1
                          ):
    """
    一、預測項目：前年至次年各期數據、預測說明、趨勢起日、數據頻率、趨勢方向、趨勢斜率、趨勢說明。
    二、預設關閉不顯示數據名稱，開啟則預測說明不帶入數據名稱，
        俾於附加其他同數據分析說明後不重覆提及數據名稱。
    """
    from zhongwen.時 import 取前年至次年間, 取民國月份, 取民國年度, 取正式民國日期
    from twse_crawler.股票基本資料分析 import 查股票代號
    from prophet.plot import add_changepoints_to_plot
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取最簡約數
    import matplotlib.pyplot as plt
    from prophet import Prophet
    import pandas as pd
    import numpy as np
    應顯示細節者 = 輔助數據欄名 == '截至次年各季台積電資本支出'

    結果數據 = pd.Series()
    歷史數據.sort_values(取樣時間欄名, inplace=True)
    if not 數據名稱:
        數據名稱 = 數據欄名.replace('本月', '').replace('月', '')

    if (n:=len(歷史數據)) == 0:
        raise 數據不足(f'歷史{數據名稱}', n, 1, f'預測次年{數據名稱}')
    try:
        取樣頻率 = 歷史數據[取樣時間欄名].dtype.freq.name
    except AttributeError:
        取樣頻率 = 取樣頻率
    歷史數據 = 歷史數據.dropna(subset=取樣時間欄名)
    歷史數據['ds'] = 歷史數據[取樣時間欄名].map(
                lambda 期間: pd.to_datetime(期間) if isinstance(期間, pd.Timestamp) else 期間.end_time.normalize()) 
    歷史數據['y'] = 歷史數據[數據欄名]

    最早數據取樣日期 = 歷史數據.ds.min()
    try:
        last_row = 歷史數據.dropna(subset=['y']).iloc[-1]
        最近數據取樣日期 = last_row.ds
    except IndexError:
        最近數據取樣日期 = 歷史數據.ds.max()

    前年至次年各期末 = 取前年至次年間(取樣頻率)
    try:
        尚未取樣時期 = [m for m in 前年至次年各期末 if m > 最近數據取樣日期]
    except TypeError as e:
        return None, f'因{e}，尚無法預測至次年{數據名稱}數據', np.nan, f'因{數據名稱}數據數少於2筆，尚無法預測趨勢'

    尚未取樣期數 = len(尚未取樣時期)
    預測模型 = Prophet(n_changepoints=變化點數, seasonality_mode=季節性模式)
    try:
        if 輔助數據欄名:
            預測模型.add_regressor(輔助數據欄名)
            預測模型.fit(歷史數據)
            預測期間 = 預測前年至次年輔助數據
            預測期間['ds'] = 預測期間.index
            # if 應顯示細節者: 顯示(預測期間)
            # if 應顯示細節者: breakpoint()
        else:
            預測模型.fit(歷史數據)
            預測期間 = 預測模型.make_future_dataframe(periods=len(尚未取樣時期), freq=取樣頻率)
        預測結果 = 預測模型.predict(預測期間)
    except ValueError as e:
        raise 數據不足(f'非空值歷史{數據名稱}', 歷史數據.dropna(subset=[數據欄名]).shape[0], 2, f'預測次年{數據名稱}')

    try:
        前年至次年各期數據 = 歷史數據.set_index('ds').reindex(index=前年至次年各期末)
    except ValueError:
        # 去除重複時間數據
        歷史數據 = 歷史數據[~歷史數據.duplicated('ds', keep='last')]
        前年至次年各期數據 = 歷史數據.set_index('ds').reindex(index=前年至次年各期末)

    前年至次年預測結果 = 預測結果.set_index('ds').reindex(index=前年至次年各期末)
    前年至次年各期數據 = pd.concat([前年至次年各期數據, 前年至次年預測結果], axis=1)
    前年至次年各期數據['數據'] = 前年至次年各期數據.y.fillna(前年至次年各期數據.yhat)
    # 顯示(前年至次年各期數據[['數據', 'y', 'yhat']], 顯示索引=True)

    取樣頻率名稱 = {'ME':'月份'
                   ,'QE':'季度', 'QE-DEC':'季度'
                   ,'YE':'年度', 'YE-DEC':'年度'
                   }[取樣頻率]
    if 取樣頻率名稱=='年度':
        最早數據時間 = 取民國年度(最早數據取樣日期)
        最近數據時間 = 取民國年度(最近數據取樣日期)
        預測數據時間 = 取民國年度(預測結果.ds.max())
    else:
        最早數據時間 = 取民國月份(最早數據取樣日期)
        最近數據時間 = 取民國月份(最近數據取樣日期)
        預測數據時間 = 取民國月份(預測結果.ds.max())

    if 輔助數據欄名:
        預測說明 =  f'以{最早數據時間}至{最近數據時間}期間{數據名稱}及{輔助數據欄名}'
    else:
        預測說明 =  f'以{最早數據時間}至{最近數據時間}期間{數據名稱}'
    預測說明 += f'預測至{預測數據時間}計{尚未取樣期數}個{取樣頻率名稱}{數據名稱}'

    if 顯示趨勢圖:
        fig = 預測模型.plot(預測結果)
        a = add_changepoints_to_plot(fig.gca(), 預測模型, 預測結果)
        預測模型.plot_components(預測結果)
        plt.show()

    # 找出趨勢轉折點，即檢測趨勢一階差分符號變化
    預測結果['趨勢差異數'] = 趨勢差異數 = 預測結果.trend.diff()
    預測結果['趨勢方向'] = 趨勢方向 = np.sign(趨勢差異數)
    預測結果['趨勢方向差異'] = 趨勢方向差異 = 趨勢方向.diff().fillna(0)
    趨勢轉折點 = 預測結果[趨勢方向差異 != 0]
    if 趨勢轉折點.shape[0] > 0:
        最近趨勢轉折點 = 趨勢轉折點.iloc[-1]
    else:
        最近趨勢轉折點 = 預測結果.iloc[0]
    if 取樣頻率名稱=='年度':
        最近趨勢轉折時間 = 取民國年度(最近趨勢轉折點.ds) 
    else:
        最近趨勢轉折時間 = 取民國月份(最近趨勢轉折點.ds) 

    趨勢方向名稱 =  '成長' if 趨勢方向.iloc[-1] > 0 else '衰退'
    if 不顯示數據名稱:
        趨勢說明 = f'{最近趨勢轉折時間}轉為{趨勢方向名稱}'
    else:
        趨勢說明 = f'{最近趨勢轉折時間}{數據名稱}轉為{趨勢方向名稱}'
    try:
        最近趨勢變異點 = 預測模型.changepoints.iloc[-1]
        最近趨勢 = 預測結果.query('ds >= @最近趨勢變異點 ')
        if 取樣頻率名稱=='年度':
            最近趨勢變異時間 = 取民國年度(最近趨勢變異點) 
        else:
            最近趨勢變異時間 = 取民國月份(最近趨勢變異點) 
    except IndexError:
        最近趨勢 = 預測結果.query('ds >= @最近趨勢轉折點.ds')
        最近趨勢變異時間 = 最近趨勢轉折時間

    # 顯示(最近趨勢 , 顯示索引=True)
    趨勢斜率, 趨勢截距 = np.polyfit(range(最近趨勢.shape[0]), 最近趨勢.trend, 1)
    趨勢增減名稱 = '增加' if 趨勢方向.iloc[-1] > 0 else '減少'
    取樣頻率簡稱 = 取樣頻率名稱[:1]
    if abs(趨勢斜率)> 100:
        趨勢說明 += f'，且{最近趨勢變異時間}以來平均按{取樣頻率簡稱}{趨勢增減名稱}{取最簡約數(abs(趨勢斜率))}元'
    else:
        趨勢說明 += f'，且{最近趨勢變異時間}以來平均按{取樣頻率簡稱}{趨勢增減名稱}{abs(趨勢斜率):.02%}'

    結果數據['前年至次年各期數據'] = 前年至次年各期數據.數據
    結果數據['預測說明'] = 預測說明
    結果數據['趨勢起日'] = 最近趨勢變異時間
    結果數據['數據頻率'] = 取樣頻率簡稱
    結果數據['趨勢方向'] = 趨勢增減名稱
    結果數據['趨勢斜率'] = 趨勢斜率
    結果數據['趨勢說明'] = 趨勢說明
    結果數據['預測數據時間'] = 預測數據時間
    結果數據['尚未取樣期數'] = 尚未取樣期數
    結果數據['取樣頻率名稱'] = 取樣頻率名稱
    結果數據['數據名稱'] = 數據名稱
    return 結果數據

@functools.cache
@通知執行時間
@cache.memoize('取自結損益表', expire=12*60*60)
def 取自結損益表(股票=None):
    '''
    一、欄位：公司代號、公司簡稱、自結損益月份、差異原因說明、
              本月營業損益、本年累計營業損益、
              本月稅前損益、本年累計稅前損益、
              本月合併營業損益、本年累計合併營業損益、
              本月合併稅前損益、本年累計合併稅前損益
    二、主鍵：公司代號及自結損益月份。
    三、可傳回指定股票並依自結損益月份由遠自近排序之自結損益表。
    四、未指定股票傳回全部股票歷月自結損益表。
    '''
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表
    from twse_crawler.公開資訊觀測站爬蟲 import 月自結損益彙總庫
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.庫 import 批次載入
    from zhongwen.時 import 本月, 上月
    from zhongwen.表 import 表示
    if 股票:
        df = 取自結損益表()
        公司代號 = 查股票代號(股票)
        df = df.query('公司代號==@公司代號')
        df['自結損益月份索引'] = df.自結損益月份
        df = df.set_index('自結損益月份索引')
        return df.sort_index()
    df = 批次載入(月自結損益彙總庫
                 ,'月自結損益彙總表', '自結損益月份'
                 ,期間欄位='自結損益月份'
                 )
    # 豐興2013年12月有個別及合併2筆紀錄，刪除個別紀錄。
    df = df.query("not (公司簡稱 == '豐興' and 本月合併稅前損益.isna())")
    dfm = 與本月相距小於三個月者 = df.groupby('公司代號')[['自結損益月份']].max()
    dfm['與本月相距月數'] = dfm.自結損益月份.map(lambda m: (本月-m).n)
    dfm = dfm.query('與本月相距月數<3')
    df = df.query('公司代號 in @dfm.index')
    return df

@通知執行時間
@增加快取時序分析結果(取自結損益表, '公司簡稱', '自結損益月份', 歷史月稅前損益分析結果快取檔, '自結損益')
def 分析月稅前損益(股票, 重新分析=True):
    '一、分數、評語、自結損益日期、股票'
    from zhongwen.時 import 今日
    from zhongwen.表 import 顯示
    import pandas as pd
    df = 歷月自結損益 = 取自結損益表(股票)
    if df.empty:
        return pd.Series()
    # 顯示(df)
    最近稅前損益 = 歷月自結損益.iloc[-1]
    # if 今日 - 最近稅前損益.自結損益月份.
    屬合併損益 = True
    if 最近稅前損益.本月合併稅前損益 > 0:
        結果 = 分析稅前利益(歷月自結損益)
    elif 最近稅前損益.本月合併稅前損益 < 0:
        結果 = 分析稅前損失(歷月自結損益)
    elif 最近稅前損益.本月稅前損益 > 0:
        屬合併損益 = False
        結果 = 分析稅前利益(歷月自結損益, 屬合併損益=False)
    elif 最近稅前損益.本月稅前損益 < 0:
        屬合併損益 = False
        結果 = 分析稅前損失(歷月自結損益, 屬合併損益=False)
    else:
        m = f'分析{最近稅前損益.公司簡稱}稅前損益出現例外：{str(最近稅前損益)}'
        raise Exception(m)
    結果['自結損益日期'] = 最近稅前損益.自結損益月份.end_time.normalize()
    結果['股票'] = 股票
    return 結果

def 分析稅前利益(歷月自結損益, 屬合併損益=True):
    '一、分數、評語'
    from zhongwen.數 import 取數值
    from 股票分析.趨勢分析 import 分析歷月數據增減情形
    from twse_crawler.股票基本資料分析 import 取股票基本資料彙總表
    import pandas as pd
    df = 歷月自結損益
    df = df.dropna(axis='columns', how='all')
    if 屬合併損益:
        稅前損益名稱 = '本月合併稅前損益'
        營業損益名稱 = '本月合併營業損益'
    else:
        稅前損益名稱 = '本月稅前損益'
        營業損益名稱 = '本月營業損益'

    r  = 損益增減情形     = 分析歷月數據增減情形(df
                                                  ,稅前損益名稱
                                                  ,'自結損益月份'
                                                  ,數據名稱='稅前損益'
                                                  )
    說明 = r.說明
    分數 = 0
    結果 = pd.Series()

    try:
        df[營業損益名稱]
    except KeyError: # 如彰銀無自結營業損益
        if r.連續增減月數 >= 3: 
            分數 += 1000
        elif r.連續增減月數 <= -3:
            分數 -= 1000
        結果['分數'] = 分數
        結果['評語'] = 說明
        return 結果

    r2 = 營業損益增減情形 = 分析歷月數據增減情形(df
                                                  ,營業損益名稱
                                                  ,'自結損益月份'
                                                  ,表達數據月份=False
                                                  ,數據名稱='營業損益'
                                                  )
    # 業外損益轉為每股業外損益表達
    公司代號 = df.iloc[-1].公司代號
    df1 = 取股票基本資料彙總表()
    股數 = df1.query("公司代號==@公司代號").iloc[-1].股數
    df['業外損益'] = df[稅前損益名稱] - df[營業損益名稱]
    df['業外損益'] = df.業外損益/股數
    r3 = 業外損益增減情形 = 分析歷月數據增減情形(df
                                                  ,'業外損益'
                                                  ,'自結損益月份'
                                                  ,表達數據月份=False
                                                  ,表達浮點數值=True
                                                  )

    if r.連續增減月數*r2.未依本月損益轉換連續增減月數 > 0:
        說明 = f'{說明}，主要係{r2.說明}'
        if r.連續增減月數 >= 3: # 本業衰退更嚴重
            分數 += 1000
        elif r.連續增減月數 <= -3:
            分數 -= 1000
    elif r.連續增減月數*r3.未依本月損益轉換連續增減月數 > 0:
        說明 = f'{說明}，主要係{r3.說明}，須仔細分析業外損益產生原因'
        if r.連續增減月數 >= 3:
            分數 += 500
        elif r.連續增減月數 <= -3:
            分數 -= 500
    else:
        m = f"未考慮情形：稅前利益增減{r.連續增減月數}次，營業損益增減{r2.連續增減月數}次"
        m+= f"，業外損益增減{r3.未依本月損益轉換連續增減月數}次"
        raise(Exception(m))

    結果 = pd.Series()
    結果['分數'] = 分數
    結果['評語'] = 說明
    return 結果

def 分析稅前損失(歷月自結損益, 屬合併損益=True):
    '一、分數、評語'
    import pandas as pd
    from 股票分析.趨勢分析 import 分析歷月數據增減情形
    df = 歷月自結損益
    df = df.dropna(axis='columns', how='all')
    if 屬合併損益:
        稅前損益名稱 = '本月合併稅前損益'
        營業損益名稱 = '本月合併營業損益'
    else:
        稅前損益名稱 = '本月稅前損益'
        營業損益名稱 = '本月營業損益'

    r  = 損益增減情形     = 分析歷月數據增減情形(df
                                                  ,稅前損益名稱
                                                  ,'自結損益月份'
                                                  ,數據名稱='稅前損益'
                                                  )
    說明 = r.說明
    分數 = 0
    結果 = pd.Series()

    try:
        r[營業損益名稱]
    except KeyError: # 如彰銀無自結營業損益
        if r.連續增減月數 >= 3: # 近一季損失連續增加
            分數 -= 2000
        elif r.連續增減月數 <= -3: # 近一季損失連續減少
            分數 -= 1000
        結果 = pd.Series()
        結果['分數'] = 分數
        結果['評語'] = 說明
        return 結果

    r2 = 營業損益增減情形 = 分析歷月數據增減情形(df
                                                  ,營業損益名稱
                                                  ,'自結損益月份'
                                                  ,表達數據月份=False
                                                  ,數據名稱='營業損益'
                                                  )
    df['業外損益'] = df[稅前損益名稱] - df[營業損益名稱]
    r3 = 業外損益增減情形 = 分析歷月數據增減情形(df
                                                  ,'業外損益'
                                                  ,'自結損益月份'
                                                  ,表達數據月份=False
                                                  )

    說明 = r.說明
    分數 = 0
    if r.未依本月損益轉換連續增減月數*r2.未依本月損益轉換連續增減月數 > 0:
        說明 = f'{說明}，主要係{r2.說明}'
        if r.連續增減月數 >= 3: # 損失連續增加
            分數 -= 2000
        elif r.連續增減月數 <= -3: # 損失連續減少
            分數 -= 500
    elif r.未依本月損益轉換連續增減月數*r3.未依本月損益轉換連續增減月數 > 0:
        說明 = f'{說明}，主要係{r3.說明}，須仔細分析業外損益產生原因'
        if r.連續增減月數 >= 3: # 損失連續增加
            分數 -= 1000
        elif r.連續增減月數 <= -3: # 損失連續減少
            分數 -= 300
    else:
        m = f"未考慮情形：稅前利益增減{r.連續增減月數}次，營業損益增減{r2.連續增減月數}次"
        m+= f"，業外損益增減{r3.未依本月損益轉換連續增減月數}次"
        raise(Exception(m))

    結果 = pd.Series()
    結果['分數'] = 分數
    結果['評語'] = 說明
    return 結果

自結損益分析快取 = Index(str(Path.home() / '.twse_crawler' / '快取' / '自結損益分析結果快取檔'))

@通知執行時間
def 預測次年稅前損益(股票, 回滾月數=0):
    """
    一、以時序預測至次年底各月未公布月稅前損益。
    二、欄位：預估每月值、預估每季總值、預估方法說明及預估說明、
        最近歷史值時間
    三、歷月營收表最近營收月份大於快取最近歷史值時間始更新。
    """
    from twse_crawler.預估至次年底每月值 import 預估至次年底每月值丙式
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    from zhongwen.表 import 表示
    from zhongwen.快取 import 停止快取
    股票簡稱 = 查股票簡稱(股票)
    h = 取自結損益表(股票)
    if h.empty:
        raise ValueError(f'{股票簡稱}無自結損益資料')
    h['稅前損益'] = h.本月稅前損益.fillna(h.本月合併稅前損益)
    if 回滾月數 > 0:
        回滾月份 =  h.自結損益月份.max()
        回滾月份 -= 回滾月數
        h = h.query('自結損益月份 <= @回滾月份')
    else:
        try:
            c = 自結損益分析快取[f'預測次年稅前損益({股票})']
            if not 停止快取 and c.最近歷史值時間 >= h.index.max():
                return c
        except KeyError: pass
    h = h.稅前損益
    p = 預估至次年底每月值丙式(h)
    p['預估方法說明'] = 表達預估方法丙(p, '稅前損益', 時間單位='月')
    p['預估說明'] = 表達預估說明丙(p, '稅前損益', 時間單位='月')
    自結損益分析快取[f'預測次年稅前損益({股票})'] = p
    return p

@通知執行時間
def 以稅前損益預測次年淨利(股票, 回滾月數=0, 估算模型誤差率=True):
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號
    from twse_crawler.預估至次年底每季值 import 表達預估方法丙, 表達預估說明丙
    from twse_crawler.營收分析 import 取預測盈餘說明
    from twse_crawler.財報分析 import 取財報彙總表
    from twse_crawler.無腦預測至次年底每季值 import calc_wmape
    from zhongwen.快取 import 刪除指定名稱快取
    from zhongwen.表 import 表示, 數據不足
    import pandas as pd
    公司代號 = 查股票代號(股票)
    公司簡稱 = 查股票簡稱(股票)

    歷季損益表 = 取財報彙總表(公司簡稱)
    if 估算模型誤差率:
        y_true = 歷季損益表.淨利.iloc[-4:]
        稅前損益預測次年淨利結果 = 以稅前損益預測次年淨利(股票, 回滾月數=12, 估算模型誤差率=False)
        y_pred = 稅前損益預測次年淨利結果.預估各季值
        try:
            y_pred = y_pred.loc[y_true.index]
        except Exception as e:
            表示(y_true)
            表示(y_pred)
            print(e)
        模型誤差率 = calc_wmape(y_true, y_pred)
        from twse_crawler.預估至次年底每季值 import 預估至次年底每季值丙式
        淨利預測結果 = 預估至次年底每季值丙式(歷季損益表.淨利)
        if 淨利預測結果.誤差率 < 模型誤差率:
            return 淨利預測結果
    回滾季數 = 回滾月數 // 3
    歷季損益表['稅前淨利'] = 歷季損益表.稅前淨利.fillna(0)
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

    # 預測稅前淨利
    預測稅前損益結果 = 預測次年稅前損益(股票, 回滾月數)
    預測稅前損益方法說明 = 表達預估方法丙(預測稅前損益結果, '稅前損益')
    預測稅前損益 = 預測稅前損益結果.預估每季總值

    全時損益表 = 歷季損益表.copy()
    future_index = 預測稅前損益.index[預測稅前損益.index > 歷季損益表.index.max()]
    全時損益表 = 全時損益表.reindex(全時損益表.index.append(future_index))
    全時損益表['稅前淨利'] = 全時損益表.稅前淨利.fillna(預測稅前損益)
    預測稅前損益未來值 = 預測稅前損益[future_index]
    # 全時損益表['稅前淨利'] = 全時損益表.稅前淨利.fillna(預測稅前損益+預測業外損益)
    # from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值

    # 預測淨利結果 = 以單元迴歸預估至次年底每季值(歷季損益表.淨利
    #                                            ,歷季損益表.稅前淨利
    #                                            ,預測稅前損益未來值
    #                                            )

    # 全時損益表['淨利'] = 全時損益表.淨利.fillna(預測淨利結果.預估各季值)

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
    try:
        預估方法說明 = (f'{表達預估方法丙(預測稅前損益結果)}'
             f'，加總之稅前損益'
             f'，扣除最高稅率20％之營所稅之損益'
             )
    except Exception as e:
        表示(預測稅前損益結果)
        raise Exception(f'預估方法說明發生：{e}')

    預測結果 = pd.Series({"預估各季值": 預估損益表.淨利,
                          "誤差率": 模型誤差率 if 估算模型誤差率 else None,
                          "歷史值數量": 預測稅前損益結果.歷史值數量,
                          "預估值數量": 預測稅前損益結果.預估值數量,
                          "回測資料數": 4,
                          "最近歷史值時間": 預測稅前損益結果.最近歷史值時間,
                          "最後預估值時間": 預測稅前損益結果.最後預估值時間,
                          "最近歷史值同比": 最近歷史值同比,
                          "首期預估值同比": 首期預估值同比,
                        })

    模型名稱 = "以稅前損益預測次年淨利"
    預測結果['模型名稱'] = 模型名稱
    預估說明 = ''
    預估說明 += f'【稅前損益預測】{表達預估說明丙(預測稅前損益結果, '稅前損益', '月')}'
    預估說明 += f'【淨利預測】{表達預估說明丙(預測結果, 預估目標='淨利')}'
    預測結果['各項預估說明'] = 預估說明
    return 預測結果

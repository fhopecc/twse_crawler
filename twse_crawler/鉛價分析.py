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

@cache.memoize('預測次年底鉛價', expire=24*60*60)
def 預測次年底鉛價():
    '''
    一、傳回預估價、預估季價、模型準確度說明。
    二、預估季價欄位：季均價、季起日價、季迄日價、季起迄價差
    三、預估季價為歷史加各季預估值。
    '''
    from twse_crawler.預估次年底 import 預估次年底價格
    return 預估次年底價格(取鉛價().現價)

def 預測次年底營收(股票):
    """
    一、傳回預估每月值、預估每季總值及模型準確度說明
    """
    from twse_crawler.預估次年底 import 預估至次年底每月值
    from twse_crawler.營收分析 import 取歷月營收表
    df = 取歷月營收表(股票).set_index('營收月份').營收
    return 預估至次年底每月值(df)

@cache.memoize('以鉛價預測次年底毛利率', expire=24*60*60)
def 以鉛價預測次年底毛利率(股票):
    """
    一、傳回預估季值、模型準確度說明
    二、預估季值為歷史加各季預估值。
    """
    from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    from twse_crawler.財報分析 import 取財報彙總表
    預估鉛季價 = 預測次年底鉛價()
    鉛價模型準確度 = 預估鉛季價.模型準確度說明
    預估鉛季價 = 預估鉛季價.預估季價[['季均價', '季起迄價差']]
    df = 取財報彙總表(股票).set_index('財報日期').to_period('Q')
    r = 依外部季數據預估次年底數值(df.毛利率, 預估鉛季價)
    r['模型準確度說明'] = (f'以{鉛價模型準確度}模型預測之鉛價'
                           f'，輸入以{r.模型準確度說明}模型預測毛利率'
                          )
    return r

def 以商品價預測至次年底各季財務數據(股票, 商品每日現價, 日期='財報日期'
                                    ,數據='毛利率', 單位='%'
                                    ,商品='鉛', 商品單位='美元'
                                    ):
    '''
    一、商品每日現價必須包含日期及現價欄位。
    二、傳回每季預測值及模型預測力。
    '''
    from twse_crawler.財報分析 import 取財報彙總表
    from sklearn.metrics import r2_score, mean_absolute_percentage_error
    import logging
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm

    # 徹底關閉 Optuna 的日誌，保持畫面乾淨
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    df_daily_lead = 商品每日現價.copy()
    _, future_dates, m = 預測至次年度商品價(df_daily_lead, 商品, 商品單位)

    df = 取財報彙總表(股票) 
    df_quarterly_margin = pd.DataFrame({"ds": df[日期], "margin": df[數據]})
    
    # 確保型態正確
    df_daily_lead["日期"] = pd.to_datetime(df_daily_lead["日期"])
    df_quarterly_margin["ds"] = pd.to_datetime(df_quarterly_margin["ds"])
    
    # ==========================================
    # 1. Pandas 特徵工程：將日資料轉化為「季特徵」
    # ==========================================
    df_daily_lead["quarter_end"] = df_daily_lead["日期"] + pd.offsets.QuarterEnd(0)
    
    df_quarterly_features = (
        df_daily_lead.groupby("quarter_end")
        .agg(
            price_mean=("現價", "mean"),
            price_start=("現價", "first"),
            price_end=("現價", "last"),
        )
        .reset_index()
    )
    
    df_quarterly_features["price_drop_effect"] = (
        df_quarterly_features["price_end"] - df_quarterly_features["price_start"]
    )
    
    # 資料合併與對齊
    df_train = pd.merge(
        df_quarterly_margin,
        df_quarterly_features,
        left_on="ds",
        right_on="quarter_end",
        how="inner",
    )
    
    # 將時間轉換為標準季度索引，確保 statsmodels 能識別頻率
    df_train = df_train.sort_values("ds")
    df_train.index = pd.DatetimeIndex(df_train["ds"]).to_period('Q')
    
    # 定義應變數 y 與外生變數 exog 矩陣
    y_series = df_train["margin"].astype(float)
    exog_df = df_train[["price_mean", "price_drop_effect"]].astype(float)
    
    # 劃分訓練集與驗證集 (保留最後1年，即4個季度作為模擬考)
    train_len = len(df_train) - 4
    train_y = y_series.iloc[:train_len]
    train_exog = exog_df.iloc[:train_len]
    
    val_y = y_series.iloc[train_len:]
    val_exog = exog_df.iloc[train_len:]
    
    # ==========================================
    # 2. 定義 Optuna 的目標尋優函數 (針對 SARIMAX)
    # ==========================================
    def objective(trial):
        # 尋找非季節性超參數 (p, d, q)
        p = trial.suggest_int('p', 0, 2)
        d = trial.suggest_int('d', 0, 1)  # 財務比率通常 0 或 1 階差分即可
        q = trial.suggest_int('q', 0, 2)
        
        # 尋找季節性超參數 (P, D, Q)，季度週期固定為 4
        P = trial.suggest_int('P', 0, 1)
        D = trial.suggest_int('D', 0, 1)
        Q = trial.suggest_int('Q', 0, 1)
        
        try:
            model = sm.tsa.statespace.SARIMAX(
                train_y,
                exog=train_exog,
                order=(p, d, q),
                seasonal_order=(P, D, Q, 4),
                enforce_stationarity=False,
                enforce_invertibility=False
            )
            results = model.fit(disp=False)
            
            # 對驗證集進行外推預測 (模擬考)
            forecast = results.forecast(steps=len(val_y), exog=val_exog)
            
            # 計算驗證指標
            rmse = np.sqrt(np.mean((val_y.values - forecast.values) ** 2))
            r2 = r2_score(val_y.values, forecast.values)
            mape = mean_absolute_percentage_error(val_y.values, forecast.values)
            
            trial.set_user_attr("rmse", float(rmse))
            trial.set_user_attr("r2", float(r2))
            trial.set_user_attr("mape", float(mape))
            return rmse
            
        except:
            # 遭遇特定無法收斂的矩陣參數時，回傳極大值懲罰
            return float('inf')

    # ==========================================
    # 3. 執行 Optuna 自動超參數優化
    # ==========================================
    print("開始啟動 Optuna 貝氏優化調參（SARIMAX模式）...")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=30)  # SARIMAX 參數空間小，30次即可收斂

    # ==========================================
    # 4. 使用最佳參數重新訓練完整 100% 的資料
    # ==========================================
    bp = study.best_params
    btua = study.best_trial.user_attrs
    
    final_model = sm.tsa.statespace.SARIMAX(
        y_series,
        exog=exog_df,
        order=(bp['p'], bp['d'], bp['q']),
        seasonal_order=(bp['P'], bp['D'], bp['Q'], 4),
        enforce_stationarity=False,
        enforce_invertibility=False
    )
    final_results = final_model.fit(disp=False)
     
    # 串接您的自訂說明格式
    參數說明 = f'依{m}，運用' 
    參數說明 += 表達模型多步預測力(btua['rmse'], btua['mape'], 單位)
    參數說明 += f'，最佳階數(p,d,q)=({bp["p"]},{bp["d"]},{bp["q"]})' 
    參數說明 += f'，季節階數(P,D,Q)_4=({bp["P"]},{bp["D"]},{bp["Q"]})' 
    參數說明 += f'之SARIMAX模型預測{數據}' 

    # ==========================================
    # 5. 處理未來預測區間的外生變數與對齊
    # ==========================================
    # 調整未來時間軸的欄位名稱以符合您的原腳本邏輯
    future_dates = future_dates.rename(columns={'quarter_end': 'ds'})
    future_dates = future_dates.sort_values("ds")
    future_dates.index = pd.DatetimeIndex(future_dates["ds"]).to_period('Q')
    
    # 擷取預測區間所需的未來商品外生變數
    # 排除與歷史重疊的部分，只留下未來的季度
    train_end_period = y_series.index[-1]
    future_exog = future_dates.loc[future_dates.index > train_end_period][["price_mean", "price_drop_effect"]].astype(float)
    
    # 執行外推預測
    steps = len(future_exog)
    forecast_final_values = final_results.forecast(steps=steps, exog=future_exog)
    
    # 【改動關鍵】：使用 how='end' 將 PeriodIndex 轉為當季的最後一天（如 2026-06-30）
    # 並用 .dt.date 去除後方的時分秒
    future_end_dates = future_exog.index.to_timestamp(how='end').normalize().date
    
    # 將預測結果整合回與原 Prophet 類似的 DataFrame 格式（ds 即為精準的季尾日期）
    forecast_final = pd.DataFrame({
        "ds": future_end_dates,
        "yhat": forecast_final_values.values
    }, index=future_exog.index)

    result_series = pd.Series({
        "每季預測值": forecast_final,
        "預測說明": 參數說明
    })
    return result_series

@通知執行時間
def 以鉛價預測次年每股盈餘(股票, 歷月營收表=None):
    '''
    一、運用指定「股票」歷月營收表預測前年至次年每股盈餘。
    二、預測結果：前年至次年每股盈餘、預測說明、營收趨勢起日、營收數據頻率、
        營收趨勢方向、營收趨勢斜率、營收趨勢說明%
    三、股票須公布2個月以上營收，否則產生「數據不足」例外。
    四、以股票快取預測結果，如快取日期落後最新營收日期時更新。
    五、增加預測上季每股盈餘及實際每股盈餘差異。
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
    預測毛利率 = 預測毛利率結果.預估季值
    歷季損益表['毛利率'] = 歷季損益表.毛利率.fillna(預測毛利率)
    歷季損益表['毛利'] = 歷季損益表.毛利.fillna(預測營收*預測毛利率)
    from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    預測營利結果 = 依外部季數據預估次年底數值(歷季損益表.營利.dropna()
                                             ,歷季損益表[['毛利']]
                                             ,預估目標='營利', 單位='元')
    預測營利 = 預測營利結果.預估季值
    歷季損益表['營利'] = 歷季損益表.營利.fillna(預測營利)
    from twse_crawler.預估次年底 import 預估至次年底每季值
    預測業外損益結果 = 預估至次年底每季值(歷季損益表.業外損益.dropna())
    預測業外損益 = 預測業外損益結果.預估每季值
    歷季損益表['業外損益'] = 歷季損益表.業外損益.fillna(預測業外損益)
    歷季損益表['稅前淨利'] = 歷季損益表.稅前淨利.fillna(預測營利+預測業外損益)

    歷季損益表['淨利'] = 歷季損益表.淨利.fillna(歷季損益表.稅前淨利*0.8)
    q = 取股票基本資料彙總表(股票)
    股數 = q.股數.iloc[-1]
    歷季損益表['每股盈餘'] = 歷季損益表.每股盈餘.fillna(歷季損益表.淨利/股數)
    表示(歷季損益表)
    from zhongwen.時 import 前年至次年各季末
    前年至次年各季數據 = 歷季損益表.reindex(index=前年至次年各季末.to_period('Q'))
    年度每股盈餘 = 前年至次年各季數據.每股盈餘.resample('Y').sum()
    from twse_crawler.預估次年底 import 表達預測準確度
    m = (f'以{表達預測準確度(預測營收結果, 單位="元")}模型預測之營收'
         f'，乘上{預測毛利率結果.模型準確度說明}之毛利'
         f'，輸入以{預測營利結果.模型準確度說明}模型預測之營利'
         f'，與以{預測業外損益結果.模型準確度說明}模型預測之業外損益'
         f'，加總之稅前損益'
         f'，扣除最高稅率20％之營所稅結果'
         f'，{取預測盈餘說明(年度每股盈餘, 前年至次年各季數據)}'
        )
    預測結果 = pd.Series({'前年至次年每股盈餘': pd.Series(年度每股盈餘)
                         ,'預測說明':m
                         })
    return 預測結果

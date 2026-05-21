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

def 表達模型多步預測力(rmse, mape, 單位='%') -> str:
    from zhongwen.數 import 最簡約數
    if 單位=='%':
        m  = f'誤差範圍{rmse:,.2%}，'
    else:
        if rmse > 10**4:
            m  = f'誤差範圍{最簡約數(rmse)}{單位}，'
        else:
            m  = f'誤差範圍{rmse:,.2f}{單位}，'
    m += f'誤差率{mape:,.2f}'
    return m

@functools.cache
@結果批次寫入(鉛價庫, '鉛價', '年度數', list(range(2008, 今年數+1)))
def 抓取年度鉛價(年度數):
    'LME 交易價'
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

def 取鉛價():
    from zhongwen.庫 import 批次載入
    return 批次載入(鉛價庫, '鉛價', '年度數', 時間欄位='日期', 起始批號=2008).sort_values('日期')

@通知執行時間
def 以鉛價預測前年至次年每股盈餘(股票, 歷月營收表=None):
    '''
    一、運用指定「股票」歷月營收表預測前年至次年每股盈餘。
    二、預測結果：前年至次年每股盈餘、預測說明、營收趨勢起日、營收數據頻率、
        營收趨勢方向、營收趨勢斜率、營收趨勢說明%
    三、股票須公布2個月以上營收，否則產生「數據不足」例外。
    四、以股票快取預測結果，如快取日期落後最新營收日期時更新。
    五、增加預測上季每股盈餘及實際每股盈餘差異。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.自結損益 import 預測前年至次年每股盈餘 as 依自結損益預測每股盈餘 
    from twse_crawler.自結損益 import 預測前年至次年周期數據
    from twse_crawler.營收分析 import 預測前年至次年營收, 取預測盈餘說明
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
    營收預測結果 = 預測前年至次年營收(股票)
    預測說明 = 營收預測結果.預測說明
    趨勢說明 = 營收預測結果.趨勢說明
    前年至次年各季營收 = 營收預測結果.前年至次年各季營收

    歷季損益表 = 取損益表(公司代號)
    try:
        最近損益 = 歷季損益表.iloc[-1]
    except IndexError as e:
        raise 數據不足(f'{公司簡稱}歷季損益', 0, 1, '預測前年至次年每股盈餘')
    except Exception as e:
        errmsg = f'{type(e).__name__}({e})'
        logger.error(errmsg)
        logger.error(公司代號)
        raise Exception(f"{公司代號}{errmsg}")

    前年數 = 今年數-1
    次年數 = 今年數+1
    前年至次年末 = pd.date_range(f'{前年數}0131', f'{次年數}1231', freq='QE')
    try:
        歷季損益表['index'] = 歷季損益表.財報日期
        歷季損益表 = 歷季損益表.set_index('index')
    except AttributeError:
        歷季損益表['財報日期'] = 歷季損益表.index

    歷季損益表['營收'] = 歷季損益表.營收.fillna(0)
    前年至次年各季數據 = 歷季損益表.reindex(index=前年至次年末)
    前年至次年各季數據['營收'] = 前年至次年各季數據.營收.fillna(前年至次年各季營收)
    歷季損益表['營收'] = 歷季損益表.營收.fillna(前年至次年各季營收).fillna(0)

    q = 取股票基本資料彙總表(股票)
    股數 = q.股數.iloc[-1]
    股數說明 = f'，再除以股票基本資料列股數{取最簡約數(股數)}股，'
    if 股數 == 0:
        股數 = 最近損益.淨利 / 最近損益.每股盈餘
        股數說明 = f'，再除以{取正式民國日期(最近損益.財報日期)}損益表推論流通股數'
        股數說明 += f'{取最簡約數(股數)}股，'
    r = 以商品價預測至次年底各季財務數據(公司代號, 取鉛價())
    預測毛利率 = r.每季預測值
    參數說明 = r.模型預測力
    預測毛利率['index'] = 預測毛利率.ds
    預測毛利率 = 預測毛利率.set_index('index').reindex(index=前年至次年末)
    前年至次年各季數據['毛利率'] = 前年至次年各季數據.毛利率.fillna(預測毛利率.yhat)
    前年至次年各季數據['毛利'] = 前年至次年各季數據.毛利.fillna(前年至次年各季數據.營收 * 前年至次年各季數據.毛利率)

    營利趨勢評分結果 = 預測前年至次年周期數據(歷季損益表, '營利', '財報期別', 取樣頻率='QE'
        ,輔助數據欄名='毛利'
        ,預測前年至次年輔助數據=前年至次年各季數據
        ,不顯示數據名稱=True
        ,顯示趨勢圖=False
        ,季節性模式='multiplicative'
        )
    鉛價日期 = 取鉛價().日期.iloc[-1]
    鉛價日期 = f'{鉛價日期.year}年{鉛價日期.month}月{鉛價日期.day}日'
    預測說明 += f'、營利、截至{鉛價日期}之鉛價'
    try:
        業外損益趨勢分析結果 = r2 = 預測前年至次年周期數據(
                歷季損益表, '業外損益', '財報期別', 取樣頻率='QE'
               ,不顯示數據名稱=True
        )
        前年至次年各季業外損益, 預測業外損益說明 = r2.前年至次年各期數據, r2.預測說明
        預測說明 += f'、業外損益'
    except AttributeError:
        # 顯示(歷季損益表)
        raise 無法預測損益(f'{公司簡稱}無業外損益數據！')
    except RuntimeError:
        raise 無法預測損益(f'{公司簡稱}業外損益有誤{歷季損益表}！')

    前年至次年各季營利 = 前年至次年各季數據['營利'] = 營利趨勢評分結果.前年至次年各期數據
    前年至次年各季業外損益 = 前年至次年各季數據['業外損益'] = 前年至次年各季數據.業外損益.fillna(前年至次年各季業外損益)
    前年至次年稅前淨利 = 前年至次年各季數據['稅前淨利'] = 前年至次年各季營利 + 前年至次年各季業外損益
    預測說明 += f'及稅前損益'

    前年至次年各季數據['淨利'] = 前年至次年各季數據.淨利.fillna(前年至次年各季數據.稅前淨利*0.8)
    預測說明 += f'，扣除最高稅率20％之營所稅'

    前年至次年各季數據['每股盈餘'] = 前年至次年各季數據.每股盈餘.fillna(前年至次年各季數據.淨利/股數)
    年度每股盈餘 = 前年至次年各季數據.每股盈餘.resample('YE').sum()
    del 營收預測結果['前年至次年各期數據']

    預測說明 += 股數說明
    預測說明 += 參數說明 + '，'
    預測說明 += 取預測盈餘說明(年度每股盈餘, 前年至次年各季數據) 
    營收預測結果['前年至次年每股盈餘'] = pd.Series(年度每股盈餘)
    營收預測結果['預測說明'] = 預測說明 
    return 營收預測結果

def 以商品價預測至次年底各季財務數據O(股票, 商品每日現價, 日期='財報日期'
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
    
    # 將預測結果整合回與原 Prophet 類似的 DataFrame 格式（保留 ds 與 yhat）
    forecast_final = pd.DataFrame({
        "ds": future_exog.index.to_timestamp(),
        "yhat": forecast_final_values.values
    }, index=future_exog.index)

    result_series = pd.Series({
        "每季預測值": forecast_final,
        "模型預測力": 參數說明
    })
    return result_series

def 預測至次年度商品價(歷史每日現價, 商品="鉛", 單位='美元'):
    """
    一、運用 Theta 方法進行狀態空間分解，自動處理季節性與趨勢，
        穩健外推至次年底的每日商品現價，並打包為季度特徵工程矩陣。
    二、歷史每日現價: 歷史必須包含日期及現價。
    三、回傳每日現價預測、每季價格及模型預測準確度。
    """
    import warnings
    import datetime
    import numpy as np
    import pandas as pd
    from statsmodels.tsa.forecasting.theta import ThetaModel
    from sklearn.metrics import r2_score, mean_absolute_percentage_error

    warnings.filterwarnings('ignore', category=UserWarning)

    # 1. 資料格式與基本檢查
    if '日期' not in 歷史每日現價.columns or '現價' not in 歷史每日現價.columns:
        raise ValueError(f"[{商品}] 輸入的 DataFrame 必須包含 '日期' 與 '現價' 欄位！")
    
    # 確保按時間正確排序，並複製一份避免改動原始資料
    df_clean = 歷史每日現價.copy()
    df_clean['日期'] = pd.to_datetime(df_clean['日期'])
    
    # 2. 強固型時間索引清洗 (防禦 statsmodels 頻率報錯)
    df_clean = df_clean.groupby('日期')['現價'].last().to_frame()
    df_clean = df_clean.sort_index()
    
    # 強制轉為日頻率並向前填充 (自動補齊週六日、國定假日等市場休市空缺)
    df_clean = df_clean.asfreq('D', method='ffill')
    
    # 明確指定日頻率標籤，這是 statsmodels 的核心命脈
    if df_clean.index.freq is None:
        df_clean.index.freq = 'D'
        
    # 3. 自動動態計算距離「次年底」還有幾天
    last_hist_date = df_clean.index.max()
    current_year = datetime.datetime.now().year
    next_year_end = datetime.datetime(current_year + 1, 12, 31)
    forecast_days = (next_year_end - last_hist_date).days
    
    if forecast_days <= 0:
        raise ValueError(f"[{商品}] 歷史資料最後一天 ({last_hist_date.strftime('%Y-%m-%d')}) 已經超越或等於次年底！")
        
    print(f"=== 啟動 [{商品}] Theta Model 預測管線 ===")
    print(f"歷史最後一天: {last_hist_date.strftime('%Y-%m-%d')} | 目標次年底: {next_year_end.strftime('%Y-%m-%d')}")
    print(f"自動計算應預測天數: {forecast_days} 天\n")

    # 4. 劃分訓練集與驗證集 (保留最後 180 天日資料作為模擬考題)
    train_series = df_clean['現價'].iloc[:-180]
    val_series = df_clean['現價'].iloc[-180:]

    # 5. 模擬回測（歷史考題評估）
    print(f"正在評估 [{商品}] Theta 模型之歷史回測表現...")
    try:
        # period=1 代表純粹趨勢與平滑分解，防禦力極高
        tm_val = ThetaModel(train_series, period=1)
        res_val = tm_val.fit()
        y_pred_val = res_val.forecast(180)
        
        # 確保預測結果長度與驗證集完全切齊
        y_pred_values = y_pred_val.values[:len(val_series)]
        
        rmse = np.sqrt(np.mean((val_series.values - y_pred_values) ** 2))
        r2 = r2_score(val_series.values, y_pred_values)
        mape = mean_absolute_percentage_error(val_series.values, y_pred_values)
    except Exception as e:
        print(f"\n[底層錯誤回報] Theta 模型歷史回測擬合失敗！錯誤訊息為: {e}")
        rmse, r2, mape = 9999.0, -1.0000, 1.0

    # 將完整的三大時序指標打包進歷史驗證描述字串中
    m = 表達模型多步預測力(rmse, mape, 單位)
    m += f'之Theta模型預測之{商品}價'
    tm_final = ThetaModel(df_clean['現價'], period=1)
    res_final = tm_final.fit()
    
    # 7. 一步到位直接外推
    future_predictions = res_final.forecast(forecast_days)
    
    # 生成未來時間軸
    future_dates = pd.date_range(start=last_hist_date + pd.Timedelta(days=1), end=next_year_end, freq='D')
    future_predictions_values = future_predictions.values[:len(future_dates)]

    # 打包未來預測日流水線
    df_future_daily = pd.DataFrame({
        "ds": future_dates,
        f"predicted_{商品}_price": future_predictions_values
    })

    # 8. 高級特徵工程：將預測日報價自動按季度分組打包
    df_future_daily["quarter_end"] = df_future_daily["ds"] + pd.offsets.QuarterEnd(0)
    
    df_future_quarterly = (
        df_future_daily.groupby("quarter_end")
        .agg(
            price_mean=(f"predicted_{商品}_price", "mean"),        
            price_start=(f"predicted_{商品}_price", "first"),      
            price_end=(f"predicted_{商品}_price", "last"),        
        )
        .reset_index()
    )
    df_future_quarterly["price_drop_effect"] = df_future_quarterly["price_end"] - df_future_quarterly["price_start"]
    result_series = pd.Series({
        "每日現價": df_future_daily,
        "每季價格": df_future_quarterly,
        "模型預測力": m
    })
    
    return result_series

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
        "模型預測力": 參數說明
    })
    return result_series


from diskcache import Cache
from pathlib import Path
from zhongwen.庫 import 結果批次寫入
from zhongwen.程式 import 通知執行時間
from zhongwen.時 import 今年數
import logging

logging.getLogger("prophet").setLevel(logging.CRITICAL)
logging.getLogger("cmdstanpy").setLevel(logging.CRITICAL)

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
鉛價庫 = Path.home() / '.twse_crawler' / '資料庫' / '鉛價庫'

毛利受鉛價影響者 = ['泰銘']

@cache.memoize('抓取年度鉛價')
@結果批次寫入(鉛價庫, '鉛價', '年度數', list(range(2008, 今年數+1)))
def 抓取年度鉛價(年度數):
    'LME 交易價'
    from zhongwen.數 import 取數值
    import pandas as pd
    from dateutil import parser
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
    return 批次載入(鉛價庫, '鉛價', '年度數', 時間欄位='日期', 起始批號=2008)

def 以大宗商品價預測至次年底各季財務數據(股票, 財務數據名稱, 大宗商品每日現價
                                        ,商品名='商品'):
    '''
    一、大宗商品每日現價必須包含日期及現價欄位。
    二、傳回模型、預測結果及參數說明。
    '''
    from twse_crawler.財報分析 import 取財報彙總表
    from sklearn.metrics import r2_score, mean_absolute_percentage_error
    import logging
    import numpy as np
    import pandas as pd
    import optuna
    from prophet import Prophet

    # 徹底關閉 Prophet、cmdstanpy 與 Optuna 的日誌，保持畫面乾淨
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    df_daily_lead = 大宗商品每日現價.copy()

    df = 取財報彙總表(股票) 
    df_quarterly_margin = pd.DataFrame({"ds": df.財報日期
                                       ,"margin": df[財務數據名稱]})
    
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
    
    df_prophet = df_train[["ds", "margin", "price_mean", "price_drop_effect"]].rename(
        columns={"margin": "y"}
    )
    
    # 劃分訓練集與驗證集 (保留最後1年作為模擬考)
    train_len = len(df_prophet) - 4
    train_df = df_prophet.iloc[:train_len]
    val_df = df_prophet.iloc[train_len:]
    
    # ==========================================
    # 2. 定義 Optuna 的目標尋優函數
    # ==========================================
    def objective(trial):
        # changepoint_prior_scale = trial.suggest_float("changepoint_prior_scale", 0.001, 0.5, log=True)
        # holidays_prior_scale = trial.suggest_float("holidays_prior_scale", 0.01, 10.0, log=True)
        changepoint_prior_scale = trial.suggest_float("changepoint_prior_scale", 0.001, 500.0, log=True)
        holidays_prior_scale = trial.suggest_float("holidays_prior_scale", 0.001, 500.0, log=True)
        
        model = Prophet(
            yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False,
            changepoint_prior_scale=changepoint_prior_scale,
            holidays_prior_scale=holidays_prior_scale,
        )
        model.add_regressor("price_mean")
        model.add_regressor("price_drop_effect")
        
        model.fit(train_df)
        
        val_X = val_df[["ds", "price_mean", "price_drop_effect"]]
        forecast = model.predict(val_X)
        
        rmse = np.sqrt(np.mean((val_df["y"].values - forecast["yhat"].values) ** 2))
        r2 = r2_score(val_df["y"].values, forecast.yhat.values)
        mape = mean_absolute_percentage_error(val_df["y"], forecast.yhat.values)
        trial.set_user_attr("rmse", float(rmse))
        trial.set_user_attr("r2", float(r2))
        trial.set_user_attr("mape", float(mape))
        return rmse

    # ==========================================
    # 3. 執行 Optuna 自動超參數優化
    # ==========================================
    print("開始啟動 Optuna 貝氏優化調參...")
    study = optuna.create_study(direction="minimize")
    study.optimize(objective, n_trials=10)

    # ==========================================
    # 4. 使用最佳參數重新訓練完整 100% 的資料
    # ==========================================
    final_model = Prophet(
        yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False,
        changepoint_prior_scale=study.best_params["changepoint_prior_scale"],
        holidays_prior_scale=study.best_params["holidays_prior_scale"],
    )
    參數說明 = f'誤差均方根為{study.best_trial.user_attrs["rmse"]:,.2%}'
    參數說明 += f'，MAPE為{study.best_trial.user_attrs["mape"]:,.2%}' 
    參數說明 += f'，r方為{study.best_trial.user_attrs["r2"]:,.2%}' 
    參數說明 += f'，趨勢轉折因子{study.best_params["changepoint_prior_scale"]:,.2f}' 
    參數說明 += f'，{商品名}影響因子{study.best_params["holidays_prior_scale"]:,.2f}' 

    final_model.add_regressor("price_mean")
    final_model.add_regressor("price_drop_effect")

    final_model = Prophet(
        yearly_seasonality=False, weekly_seasonality=False, daily_seasonality=False,
        changepoint_prior_scale=study.best_params["changepoint_prior_scale"],
        holidays_prior_scale=study.best_params["holidays_prior_scale"],
    )
    final_model.fit(df_prophet)
    
    # ==========================================
    # 5. 生成直到 2027-12-31 的未來預測時間軸
    # ==========================================
    # future_quarters = pd.date_range(start="2026-06-01", end="2027-12-31", freq="QE")
    # future_dates = pd.DataFrame({"ds": future_quarters})
    _, future_dates = 預測至次年度商品價(df_daily_lead, 商品名=商品名)
    future_dates = future_dates.rename(columns={'quarter_end':'ds'})
    # future_dates["price_mean"] = 1950.0  
    # future_dates["price_drop_effect"] = 0.0  
    # 執行預測
    forecast_final = final_model.predict(future_dates)
    return final_model, forecast_final, 參數說明

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
    鉛價 = 取鉛價().sort_values('日期')
    _, 預測毛利率, 參數說明 = 以大宗商品價預測至次年底各季財務數據(公司代號, '毛利率'
                                                                  ,鉛價 ,'鉛價')
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
    鉛價日期 = 鉛價.日期.iloc[-1]
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

def 預測至次年度商品價(歷史日價, 商品名='商品', 嘗試次數=15):
    """
    一、回傳預測日商品價、季商品價。
    """
    # ---------------------------------------------------------
    # 將所有 import 放到函式內部
    # ---------------------------------------------------------
    import logging
    import datetime
    import numpy as np
    import pandas as pd
    import optuna
    from prophet import Prophet

    # 關閉 Prophet 與 Optuna 惱人的日誌訊息，保持主程式控制台乾淨
    logging.getLogger("prophet").setLevel(logging.ERROR)
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    df_historical_daily = 歷史日價
    # 1. 資料格式檢查與標準化轉換
    if '日期' not in df_historical_daily.columns or '現價' not in df_historical_daily.columns:
        raise ValueError("輸入的 DataFrame 必須包含日期與現價欄位！")
        
    df_input = df_historical_daily.rename(columns={"日期": "ds", "現價": "y"})
    
    # 2. 自動計算距離「次年底」還有幾天
    last_hist_date = df_input["ds"].max()
    current_year = datetime.datetime.now().year  # 自動抓取目前年份 (2026年 / 115年)
    next_year_end = datetime.datetime(current_year + 1, 12, 31)  # 目標次年底 (2027年 / 116年12月31日)
    
    # 計算需要預測的總天數
    forecast_days = (next_year_end - last_hist_date).days
    
    if forecast_days <= 0:
        raise ValueError(f"歷史資料的最後一天 ({last_hist_date.strftime('%Y-%m-%d')}) 已經超越或等於次年底！請檢查數據。")
        
    print(f"歷史資料最後一天為: {last_hist_date.strftime('%Y-%m-%d')}")
    print(f"動態鎖定目標次年底: {next_year_end.strftime('%Y-%m-%d')}")
    print(f"自動計算應預測天數: {forecast_days} 天\n")

    # 3. 劃分訓練集與驗證集 (保留最後 180 天日資料作為模擬考題引導 Optuna)
    train_len = len(df_input) - 180
    train_df = df_input.iloc[:train_len]
    val_df = df_input.iloc[train_len:]
    
    # 4. 定義 Optuna 的內部尋優目標函數
    def _objective(trial):
        changepoint_prior_scale = trial.suggest_float("changepoint_prior_scale", 0.005, 0.3, log=True)
        yearly_prior_scale = trial.suggest_float("yearly_prior_scale", 0.01, 10.0, log=True)
        
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            changepoint_prior_scale=changepoint_prior_scale,
            seasonality_prior_scale=yearly_prior_scale
        )
        model.fit(train_df)
        
        forecast = model.predict(val_df[['ds']])
        rmse = np.sqrt(np.mean((val_df['y'].values - forecast['yhat'].values) ** 2))
        return rmse

    # 5. 執行第一階段的超參數尋優
    print(f"啟動日鉛價模型優化 (預計嘗試 {嘗試次數} 次組合)...")
    study = optuna.create_study(direction="minimize")
    study.optimize(_objective, n_trials=嘗試次數)
    
    print(f"優化完成！最佳參數組合: {study.best_params}")
    print(f"歷史回測 180 天之最小預測 RMSE 誤差: {study.best_value:.2f} 美元\n")
    
    # 6. 使用 Optuna 篩選出的金牌參數，完整擬合所有歷史資料
    print("正在使用最佳參數訓練最終日鉛價模型並進行長線推演...")
    final_model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        changepoint_prior_scale=study.best_params['changepoint_prior_scale'],
        seasonality_prior_scale=study.best_params['yearly_prior_scale']
    )
    final_model.fit(df_input)
    
    # 7. 生成未來時間軸並計算預測值
    future_timeline = final_model.make_future_dataframe(periods=forecast_days, freq="D")
    forecast_all = final_model.predict(future_timeline)
    
    # 僅切出純粹的「未來預測區段」
    df_future_all = forecast_all[forecast_all["ds"] > last_hist_date]
    df_future_daily = df_future_all[["ds", "yhat"]].rename(columns={"yhat": "predicted_lead_price"}).reset_index(drop=True)
    
    # 8. 高級特徵工程：將預測日報價自動按季度分組打包
    df_future_daily["quarter_end"] = df_future_daily["ds"] + pd.offsets.QuarterEnd(0)
    
    df_future_quarterly = (
        df_future_daily.groupby("quarter_end")
        .agg(
            price_mean=("predicted_lead_price", "mean"),        # 未來各季預估均價
            price_start=("predicted_lead_price", "first"),      # 未來各季預估期初價
            price_end=("predicted_lead_price", "last"),        # 未來各季預估期末價
        )
        .reset_index()
    )
    
    # 計算未來各季的預估期末跌幅
    df_future_quarterly["price_drop_effect"] = df_future_quarterly["price_end"] - df_future_quarterly["price_start"]
    
    print("次年底預測流水線處理完畢。")
    return df_future_daily, df_future_quarterly

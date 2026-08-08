def 取以單元迴歸預估至次年底每季值模型(
    歷季自變數: "pd.Series | pd.DataFrame",
    歷季應變數: "pd.Series"
) -> "pd.Series":
    """
    一、主要欄位：模型擬合、模型名稱、採用指標、誤差率、較無腦模型改善率、
                 最佳訓練資料數、回測資料數。
    二、輔助欄位：指標說明、wmape、naive_wmape、snaive_wmape、
                 _y_原始、_y_最終訓練、_X_原始、_X_最終訓練。
    三、最佳模型：係回測 4 季之評估指標最小 OLS 線性迴歸之訓練資料數。
    四、搜尋次數：固定執行 30 次 Optuna 試驗，尋找最佳訓練視窗。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 內部固定參數
    回測季數 = 4
    N_TRIALS = 30

    # 2. 數據型態檢查、防禦與自動對齊
    y = 歷季應變數.astype(float)
    
    # 確保自變數為 DataFrame 結構 (支援單一自變數或多個自變數)
    if isinstance(歷季自變數, pd.Series):
        X_原始 = 歷季自變數.to_frame()
    else:
        X_原始 = 歷季自變數.copy()
    X_原始 = X_原始.astype(float)

    # 建立強健的季度索引轉換邏輯 (支援 PeriodIndex, DatetimeIndex, str 等)
    def 轉為季度索引(idx):
        if isinstance(idx, pd.PeriodIndex):
            return idx.asfreq('Q')
        else:
            return pd.to_datetime(idx).to_period('Q')

    # 對齊 index 頻率為 Q
    try:
        y.index = 轉為季度索引(y.index)
        X_原始.index = 轉為季度索引(X_原始.index)
    except Exception as e:
        raise ValueError(f"索引無法轉換為季度型態，錯誤原因: {e}")

    # 確保 y 與 X 在歷史區間索引對齊
    共通索引 = y.index.intersection(X_原始.index)
    y = y.loc[共通索引]
    X_原始 = X_原始.loc[共通索引]

    from twse_crawler.無腦預測至次年底每季值 import calc_wmape

    # 4. 定義 Optuna 最佳化目標函數
    def objective(trial):
        min_window = 8  # 最少 8 季 (2 年)
        max_window = len(y) - 回測季數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        # 切分訓練集與驗證集
        訓練起點 = max(0, len(y) - 回測季數 - window_size)
        訓練終點 = len(y) - 回測季數

        y_train = y.iloc[訓練起點:訓練終點]
        X_train = X_原始.iloc[訓練起點:訓練終點]

        y_validation = y.iloc[訓練終點:].values  # 未來 4 季真實應變數
        X_validation = X_原始.iloc[訓練終點:]     # 驗證期的自變數

        try:
            # 加入常數項擬合 OLS
            X_train_const = sm.add_constant(X_train, has_constant='add')
            ols_model = sm.OLS(y_train, X_train_const)
            模型擬合 = ols_model.fit()

            # 預測驗證期
            X_validation_const = sm.add_constant(X_validation, has_constant='add')
            預測陣列 = 模型擬合.predict(X_validation_const).values

            if pd.isna(預測陣列).any() or np.isinf(預測陣列).any() or len(預測陣列) != 回測季數:
                return float('inf')
        except:
            return float('inf')
            
        當前_wmape = calc_wmape(y_validation, 預測陣列)
        trial.set_user_attr("wmape", float(當前_wmape))

        return 當前_wmape

    # 5. 執行 Optuna 搜尋
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=N_TRIALS)

    最佳參數 = 研究工廠.best_params
    最佳訓練資料數 = 最佳參數.get('window_size', len(y) - 回測季數)

    最佳實驗 = 研究工廠.best_trial
    best_wmape = 最佳實驗.user_attrs.get("wmape", np.inf)

    # 6. 與不以外部變數 X 預測，僅以 y 時序預測比較，取出較佳者
    from twse_crawler.預估至次年底每季值 import 取預估至次年底每季值模型
    only_y_forecast = 取預估至次年底每季值模型(y)
    
    # 提取 y 時序預測的誤差率 (安全存取)
    y_wmape = getattr(only_y_forecast, 'wmape', getattr(only_y_forecast, '誤差率', np.inf))

    if y_wmape < best_wmape: # y 時序預測勝出
        誤差率 = y_wmape
        最終模型擬合 = getattr(only_y_forecast, '模型擬合', only_y_forecast)
        模型顯示名稱 = getattr(only_y_forecast, '模型名稱', '純y時序模型')
        y_best_train = y
        X_best_train = None
        is_ols = False
    else: # OLS 迴歸模型勝出
        誤差率 = best_wmape
        最終訓練起點 = max(0, len(y) - 最佳訓練資料數)
        y_best_train = y.iloc[最終訓練起點:]
        X_best_train = X_原始.iloc[最終訓練起點:]
        X_best_train_const = sm.add_constant(X_best_train, has_constant='add')
        ols_final = sm.OLS(y_best_train, X_best_train_const)
        最終模型擬合 = ols_final.fit()
        模型顯示名稱 = "OLS"
        is_ols = True

    return pd.Series({
        "模型擬合": 最終模型擬合,
        "模型名稱": 模型顯示名稱,
        "誤差率": 誤差率,
        "最佳訓練資料數": 最佳訓練資料數,
        "回測資料數": 回測季數,
        "is_ols": is_ols,
        "_y_原始": y,
        "_y_最終訓練": y_best_train,
        "_X_原始": X_原始,
        "_X_最終訓練": X_best_train
    })


def 以單元迴歸預估至次年底每季值(
    歷季自變數: "pd.Series | pd.DataFrame",
    歷季應變數: "pd.Series",
    未來自變數: "pd.Series | pd.DataFrame",
    單位 = '元'
) -> "pd.Series":
    """
    一、預測結果：預估各季值、最近歷史值同比、首期預估值同比、誤差率、較無腦模型改善率、
                 趨勢、判定係數R2。
    二、預測方法：模型名稱、採用指標、指標說明、歷史值數量、預估值數量、回測資料數、
                 最佳訓練資料數、模型參數、最近歷史值時間、最後預估值時間。
    三、外推防禦：加入空值與發散值補強，適合業外損益與淨利等高波動財務科目。
    """
    # 1. 於函式內部進行套件導入
    import numpy as np
    import pandas as pd
    import statsmodels.api as sm

    # 2. 調用專屬 OLS 迴歸模型尋優函式
    模型結果 = 取以單元迴歸預估至次年底每季值模型(歷季自變數, 歷季應變數)
    最終模型擬合 = 模型結果["模型擬合"]
    y = 模型結果["_y_原始"]
    y_best_train = 模型結果["_y_最終訓練"]
    回測季數 = 模型結果["回測資料數"]
    is_ols = 模型結果["is_ols"]

    # 3. 處理未來自變數結構與索引
    if isinstance(未來自變數, pd.Series):
        X_未來 = 未來自變數.to_frame()
    else:
        X_未來 = 未來自變數.copy()
    X_未來 = X_未來.astype(float)

    def 轉為季度索引(idx):
        if isinstance(idx, pd.PeriodIndex):
            return idx.asfreq('Q')
        else:
            return pd.to_datetime(idx).to_period('Q')

    try:
        X_未來.index = 轉為季度索引(X_未來.index)
    except Exception as e:
        raise ValueError(f"未來自變數索引無法轉換為季度型態，錯誤原因: {e}")

    # 自動識別真正的「未來」季度 (大於歷史 y 的最新一季)
    未來季度索引 = X_未來.index[X_未來.index > y.index[-1]]
    
    # 備援防禦：若未來自變數未包含未來季度，則自動向外延伸 len(X_未來) 季
    if len(未來季度索引) == 0:
        未來季度索引 = pd.period_range(start=y.index[-1] + 1, periods=len(X_未來), freq='Q')
        X_未來_有效 = X_未來.copy()
        X_未來_有效.index = 未來季度索引
    else:
        X_未來_有效 = X_未來.loc[未來季度索引]

    外推步數 = len(未來季度索引)

    # 4. 分支處理外推未來預測值與模型參數 (OLS vs 純 Y 時序模型)
    if is_ols:
        X_未來_const = sm.add_constant(X_未來_有效, has_constant='add')
        預測值_array = 最終模型擬合.predict(X_未來_const)
        預估季_陣列 = pd.Series(預測值_array, index=未來季度索引)
        
        # 提取 OLS 的斜率與 R2
        params = 最終模型擬合.params
        slope = params.iloc[1] if len(params) > 1 else 0.0
        rsquared = getattr(最終模型擬合, 'rsquared', np.nan)
    else:
        # 當純 Y 時序模型勝出時，呼叫時序模型原生的 forecast API
        try:
            forecast_res = 最終模型擬合.forecast(steps=外推步數)
            if isinstance(forecast_res, (pd.Series, pd.DataFrame)):
                預估季_陣列 = pd.Series(forecast_res.values, index=未來季度索引)
            else:
                預估季_陣列 = pd.Series(forecast_res, index=未來季度索引)
        except Exception:
            # 備援填補：以訓練集最近 4 季平均填補
            安全值 = y_best_train.tail(4).mean()
            預估季_陣列 = pd.Series(安全值, index=未來季度索引)
        
        slope = 0.0
        rsquared = np.nan

    # 5. 防禦機制：發散與 NaN 採最後 4 季平均值填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_best_train.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)

    預估季_序列 = 預估季_陣列

    # 6. 整合與計算各項延伸統計指標
    預估各季全序列 = pd.concat([y, 預估季_序列])

    最近歷史值同比 = (
        (y.iloc[-1] - y.iloc[-5]) / y.iloc[-5]
        if len(y) > 4 and y.iloc[-5] != 0 else np.nan
    )

    首期預估值同比 = (
        (預估季_序列.iloc[0] - y.iloc[-4]) / y.iloc[-4]
        if len(y) >= 3 and y.iloc[-4] != 0 else np.nan
    )

    最近季度 = y.index[-1]
    最後預估值時間 = 預估季_序列.index[-1]

    # 7. 回傳最終彙整 Series
    return pd.Series({
        "預估各季值": 預估各季全序列,
        "模型名稱": 模型結果["模型名稱"],
        "誤差率": 模型結果["誤差率"],
        "歷史值數量": len(y),
        "預估值數量": 外推步數,
        "回測資料數": 回測季數,
        "最佳訓練資料數": 模型結果["最佳訓練資料數"],
        "趨勢": slope,                                 # 自變數迴歸係數
        "近期影響權重": rsquared,                      # R^2 擬合優度
        "最近歷史值時間": 最近季度,
        "最後預估值時間": 最後預估值時間,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })

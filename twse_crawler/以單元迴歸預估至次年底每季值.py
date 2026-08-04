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
    best_wmape = 最佳實驗.user_attrs.get("wmape", np.nan)

    # 對外輸出的誤差率
    誤差率 = best_wmape

    # 6. 計算 Naïve 與 Seasonal Naïve 基準
    真實值 = y.iloc[-回測季數:].values
    snaive_base = y.iloc[-回測季數-4:-4].values if len(y) >= 回測季數 + 4 else 真實值

    naive_pred = np.full(回測季數, y.iloc[-回測季數-1])
    naive_wmape = calc_wmape(真實值, naive_pred)

    snaive_pred = snaive_base
    snaive_wmape = calc_wmape(真實值, snaive_pred)

    # 較無腦模型改善率計算
    無腦基線最優誤差 = min(naive_wmape, snaive_wmape)

    if 無腦基線最優誤差 > 0 and not np.isinf(無腦基線最優誤差):
        較無腦模型改善率 = (無腦基線最優誤差 - best_wmape) / 無腦基線最優誤差
    else:
        較無腦模型改善率 = np.nan

    # 7. 擬合最終勝出的 OLS 迴歸模型
    最終訓練起點 = max(0, len(y) - 最佳訓練資料數)
    y_best_train = y.iloc[最終訓練起點:]
    X_best_train = X_原始.iloc[最終訓練起點:]

    X_best_train_const = sm.add_constant(X_best_train, has_constant='add')
    ols_final = sm.OLS(y_best_train, X_best_train_const)
    最終模型擬合 = ols_final.fit()

    模型顯示名稱 = "OLS"

    指標說明 = (
        f"由 Optuna 多步盲測（固定 4 季回測），訓練資料數為 {最佳訓練資料數} 季，"
        f"採用 [{模型顯示名稱}]，評估指標為 [WMAPE]"
    )

    return pd.Series({
        "模型擬合": 最終模型擬合,
        "模型名稱": 模型顯示名稱,
        "指標說明": 指標說明,
        "誤差率": 誤差率,
        "wmape": best_wmape,
        "naive_wmape": naive_wmape,
        "snaive_wmape": snaive_wmape,
        "較無腦模型改善率": 較無腦模型改善率,
        "最佳訓練資料數": 最佳訓練資料數,
        "回測資料數": 回測季數,
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
    from zhongwen.時 import 今年數

    # 2. 調用專屬 OLS 迴歸模型尋優函式
    模型結果 = 取以單元迴歸預估至次年底每季值模型(歷季自變數, 歷季應變數)
    最終模型擬合 = 模型結果["模型擬合"]
    y = 模型結果["_y_原始"]
    y_best_train = 模型結果["_y_最終訓練"]
    回測季數 = 模型結果["回測資料數"]

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

    外推步數 = len(X_未來)

    # 4. 外推未來預測值 (使用未來自變數帶入模型 predict)
    X_未來_const = sm.add_constant(X_未來, has_constant='add')
    預估季_陣列 = pd.Series(最終模型擬合.predict(X_未來_const), index=X_未來.index)

    # 防禦機制：發散與 NaN 採最後 4 季平均值填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_best_train.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)

    預估季_序列 = 預估季_陣列

    # 5. 整合與計算各項延伸統計指標
    預估各季全序列 = pd.concat([y, 預估季_序列])

    # 提取第一個自變數的斜率 (趨勢) 與 R2
    params = 最終模型擬合.params
    slope = params.iloc[1] if len(params) > 1 else 0.0  # 第一個自變數的迴歸係數
    rsquared = 最終模型擬合.rsquared                     # 判定係數 R^2

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

    # 6. 回傳最終彙整 Series
    return pd.Series({
        "預估各季值": 預估各季全序列,
        "模型名稱": 模型結果["模型名稱"],
        "指標說明": 模型結果["指標說明"],
        "誤差率": 模型結果["誤差率"],
        "較無腦模型改善率": 模型結果["較無腦模型改善率"],
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

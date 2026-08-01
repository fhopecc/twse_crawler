def 取預估至次年底工作日值模型(
    歷日數值: "pd.Series"
) -> "pd.Series":
    """
    一、主要欄位：模型擬合、模型名稱、採用指標、誤差率、較無腦模型改善率、
                 最佳訓練資料數、回測資料數。
    二、輔助欄位：指標說明、wmpe、swmpe、naive_wmpe、naive_swmpe、
                 snaive_wmpe、snaive_swmpe、_y_原始、_y_最終訓練。
    三、最佳模型：係回測 30 個工作日之評估指標最小 Theta 之訓練資料數及是否去季節情形。
    四、搜尋次數：固定執行 30 次 Optuna 試驗，兼顧搜尋品質與速度。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.forecasting.theta import ThetaModel

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 內部固定參數 (一年約 252 個工作日)
    回測工作日數 = 30
    N_TRIALS = 30
    SEASON_PERIOD = 252

    # 2. 數據型態轉換、建構完整工作日序列並以 ffill 補齊
    try:
        y_tmp = 歷日數值.copy()
        y_tmp.index = pd.to_datetime(y_tmp.index).to_period('B')
    except Exception as e:
        raise ValueError(
            f"歷日數值索引無法轉換為工作日型態，實際提供資料索引為 {type(歷日數值.index)}，錯誤原因: {e}"
        )
    
    # 移除重複的索引（若有重複日期，保留最後一筆報價）
    y_tmp = y_tmp[~y_tmp.index.duplicated(keep='last')]

    # 建構從歷史第一天至最後一天無間斷的完整工作日時間軸
    完整工作日時間軸 = pd.period_range(start=y_tmp.index[0], end=y_tmp.index[-1], freq='B')
    
    # 重新對齊索引並使用 ffill 向前填補（若第一筆仍為 NaN 則以 bfill 輔助）
    y_原始 = (
        y_tmp.reindex(完整工作日時間軸)
        .ffill()
        .bfill()
        .astype(float)
    )

    # 3. 定義 WMPE 與 Seasonal WMPE (SWMPE) 計算邏輯
    def calc_wmpe(y_true, y_pred):
        sum_abs_true = np.sum(np.abs(y_true))
        if sum_abs_true == 0:
            return float('inf')
        return np.sum(np.abs(y_true - y_pred)) / sum_abs_true

    def calc_swmpe(y_true, y_pred, y_seasonal_base):
        if len(y_seasonal_base) < len(y_true):
            return calc_wmpe(y_true, y_pred)
        seasonal_diff_sum = np.sum(np.abs(y_true - y_seasonal_base))
        if seasonal_diff_sum == 0:
            return float('inf')
        return np.sum(np.abs(y_true - y_pred)) / seasonal_diff_sum

    # 4. 定義 Optuna 最佳化目標函數
    def objective(trial):
        min_window = 252  # 最少 252 個工作日 (1 年)
        max_window = len(y_原始) - 回測工作日數
        if max_window <= min_window:
            window_size = max(30, max_window) # 極端資料長度不足防禦
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        deseasonalize_choice = trial.suggest_categorical('deseasonalize', [True, False])
        metric_choice = trial.suggest_categorical('metric', ['wmpe', 'swmpe'])

        # 切分訓練集與驗證集
        訓練起點 = max(0, len(y_原始) - 回測工作日數 - window_size)
        訓練終點 = len(y_原始) - 回測工作日數
        y_訓練 = y_原始.iloc[訓練起點:訓練終點]
        y_真實驗證 = y_原始.iloc[訓練終點:].values  # 未來 30 個工作日真實值
        
        try:
            tm = ThetaModel(y_訓練, period=SEASON_PERIOD, deseasonalize=deseasonalize_choice)
            模型擬合 = tm.fit()
            預測陣列 = 模型擬合.forecast(回測工作日數).values
            
            if pd.isna(預測陣列).any() or np.isinf(預測陣列).any() or len(預測陣列) != 回測工作日數:
                return float('inf')
        except:
            return float('inf')

        snaive_base = y_原始.iloc[-回測工作日數-SEASON_PERIOD:-SEASON_PERIOD].values if len(y_原始) >= 回測工作日數 + SEASON_PERIOD else y_真實驗證
        
        當前_wmpe = calc_wmpe(y_真實驗證, 預測陣列)
        當前_swmpe = calc_swmpe(y_真實驗證, 預測陣列, snaive_base)
        
        trial.set_user_attr("wmpe", float(當前_wmpe))
        trial.set_user_attr("swmpe", float(當前_swmpe))
        
        return 當前_wmpe if metric_choice == 'wmpe' else 當前_swmpe

    # 5. 執行 Optuna 搜尋
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=N_TRIALS)
    
    最佳參數 = 研究工廠.best_params
    最佳訓練資料數 = 最佳參數.get('window_size', len(y_原始) - 回測工作日數)
    勝出季節性 = 最佳參數.get('deseasonalize', True)
    勝出指標 = 最佳參數.get('metric', 'swmpe')

    最佳實驗 = 研究工廠.best_trial
    best_wmpe = 最佳實驗.user_attrs.get("wmpe", np.nan)
    best_swmpe = 最佳實驗.user_attrs.get("swmpe", np.nan)

    # 依勝出指標決定最終對外輸出的誤差率
    誤差率 = best_wmpe if 勝出指標 == 'wmpe' else best_swmpe

    # 6. 計算 Naïve 與 Seasonal Naïve 基準
    真實值 = y_原始.iloc[-回測工作日數:].values
    snaive_base = y_原始.iloc[-回測工作日數-SEASON_PERIOD:-SEASON_PERIOD].values if len(y_原始) >= 回測工作日數 + SEASON_PERIOD else 真實值
    
    naive_pred = np.full(回測工作日數, y_原始.iloc[-回測工作日數-1])
    naive_wmpe = calc_wmpe(真實值, naive_pred)
    naive_swmpe = calc_swmpe(真實值, naive_pred, snaive_base)

    snaive_pred = snaive_base
    snaive_wmpe = calc_wmpe(真實值, snaive_pred)
    snaive_swmpe = calc_swmpe(真實值, snaive_pred, snaive_base)

    # 較無腦模型改善率計算
    模型最佳誤差 = min(best_wmpe, best_swmpe)
    無腦基線最優誤差 = min(naive_wmpe, naive_swmpe, snaive_wmpe, snaive_swmpe)
    
    if 無腦基線最優誤差 > 0 and not np.isinf(無腦基線最優誤差):
        較無腦模型改善率 = (無腦基線最優誤差 - 模型最佳誤差) / 無腦基線最優誤差
    else:
        較無腦模型改善率 = np.nan

    # 7. 擬合最終勝出的 Theta 模型
    最終訓練起點 = max(0, len(y_原始) - 最佳訓練資料數)
    y_最終訓練 = y_原始.iloc[最終訓練起點:]
    
    tm = ThetaModel(y_最終訓練, period=SEASON_PERIOD, deseasonalize=勝出季節性)
    最終模型擬合 = tm.fit()
    模型顯示名稱 = f"最小 {勝出指標.upper()} 之{'季節性' if 勝出季節性 else ''} Theta"

    指標說明 = (
        f"由 Optuna 多步盲測（固定 {回測工作日數} 個工作日回測），訓練資料數為 {最佳訓練資料數} 個工作日，"
        f"採用 [{模型顯示名稱}]，評估指標為 [{勝出指標.upper()}]"
    )

    _y_原始 = y_原始
    _y_最終訓練 = y_最終訓練

    return pd.Series({
        "模型擬合": 最終模型擬合,
        "模型名稱": 模型顯示名稱,
        "採用指標": 勝出指標,
        "指標說明": 指標說明,
        "誤差率": 誤差率,
        "wmpe": best_wmpe,
        "swmpe": best_swmpe,
        "naive_wmpe": naive_wmpe,
        "naive_swmpe": naive_swmpe,
        "snaive_wmpe": snaive_wmpe,
        "snaive_swmpe": snaive_swmpe,
        "較無腦模型改善率": 較無腦模型改善率,
        "最佳訓練資料數": 最佳訓練資料數,
        "回測資料數": 回測工作日數,
        "_y_原始": _y_原始,
        "_y_最終訓練": _y_最終訓練
    })


def 預估至次年底工作日值丙式(
    歷日數值: "pd.Series",
    單位 = '元'
) -> "pd.Series":
    """
    一、預測結果：預估各工作日值、預估季均值、預估季起日值、預估季迄日值、預估季增減數、
                 最近歷史值同比、首期預估值同比、誤差率、較無腦模型改善率、趨勢、近期影響權重。
    二、預測方法：模型名稱、採用指標、指標說明、歷史值數量、預估值數量、回測資料數、
                 最佳訓練資料數、模型參數、最近歷史值時間、最後預估值時間。
    三、外推防禦：透過 ffill 補齊歷史空缺工作日，並對預測極端值加入近 252 個工作日均值安全填充。
    四、季度採 QE 模式：自本季起算，跨界季（本季）之起日為歷史真實值、迄日為預估值。
    """
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    模型結果 = 取預估至次年底工作日值模型(歷日數值)
    最終模型擬合 = 模型結果["模型擬合"]
    y_原始 = 模型結果["_y_原始"]
    y_最終訓練 = 模型結果["_y_最終訓練"]
    回測工作日數 = 模型結果["回測資料數"]

    # 動態識別並建構工作日預測時間軸（向未來外推至次年 12 月 31 日前的最後工作日）
    最近日期 = y_原始.index[-1]
    次年數 = 今年數 + 1
    
    未來下一工作日 = 最近日期 + 1
    次年底最後工作日 = pd.Period(f"{次年數}-12-31", freq='B')
    未來工作日時軸 = pd.period_range(start=未來下一工作日, end=次年底最後工作日, freq='B')
    外推步數 = len(未來工作日時軸)

    # 外推未來預測值至次年底
    預估日_陣列 = 最終模型擬合.forecast(外推步數)
    
    # 財務保守原則：防禦發散值，採最後 252 個工作日（約一年）平均值安全填充
    if 預估日_陣列.isna().any() or np.isinf(預估日_陣列.values).any():
        安全填補值 = y_最終訓練.tail(252).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估日_陣列 = 預估日_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估日_序列 = pd.Series(預估日_陣列.values, index=未來工作日時軸)

    # 整合歷史與預估的完整工作日序列
    預估每日全序列 = pd.concat([y_原始, 預估日_序列])

    # ----------------------------------------------------
    # QE 模式季度計算 (修正 Period / PeriodIndex 轉換語法)
    # ----------------------------------------------------
    # 1. 取得本季標籤
    if isinstance(最近日期, pd.Period):
        本季標籤 = 最近日期.asfreq('Q')
    else:
        本季標籤 = pd.Period(pd.to_datetime(最近日期), freq='Q')

    # 2. 建立季度分組索引 (PeriodIndex 採 asfreq('Q')，DatetimeIndex 採 dt.to_period('Q'))
    if isinstance(預估每日全序列.index, pd.PeriodIndex):
        季分組索引 = 預估每日全序列.index.asfreq('Q')
    else:
        季分組索引 = pd.to_datetime(預估每日全序列.index).dt.to_period('Q')

    全序列季分組 = 預估每日全序列.groupby(季分組索引)

    # 3. 計算全序列的季統計指標
    全季均值 = 全序列季分組.mean()
    全季起日值 = 全序列季分組.first()
    全季迄日值 = 全序列季分組.last()
    全季增減數 = 全季迄日值 - 全季起日值

    # 4. 僅截取「自本季起」至「次年底」的季度結果
    次年底季標籤 = pd.Period(f"{次年數}Q4", freq='Q')
    # 本季至次年底篩選 = (全季均值.index >= 本季標籤) & (全季均值.index <= 次年底季標籤)
    
    # 季均值 = 全季均值[本季至次年底篩選]
    # 季起日值 = 全季起日值[本季至次年底篩選]
    # 季迄日值 = 全季迄日值[本季至次年底篩選]
    # 季增減數 = 全季增減數[本季至次年底篩選]
    # 預估季均值 = pd.concat([季均值, 季起日值, 季迄日值, 季增減數]
    預估季均值 = pd.concat([全季均值, 全季起日值, 全季迄日值, 全季增減數]
                          ,keys=["季均值", "季起日值", "季迄日值", "季增減數"]
                          ,axis=1)
    # ----------------------------------------------------

    模型參數字典 = pd.Series({
        "b0": 最終模型擬合.params.b0,
        "alpha": 最終模型擬合.params.alpha
    })
    
    # 同比為與上年同工作日相比 (約相差 252 個工作日)
    最近歷史值同比 = (
        (y_原始.iloc[-1] - y_原始.iloc[-253]) / y_原始.iloc[-253] 
        if len(y_原始) > 252 and y_原始.iloc[-253] != 0 else np.nan
    )
    
    首期預估值同比 = (
        (預估日_序列.iloc[0] - y_原始.iloc[-252]) / y_原始.iloc[-252] 
        if len(y_原始) >= 252 and y_原始.iloc[-252] != 0 else np.nan
    )

    return pd.Series({
        "預估每日值": 預估每日全序列,
        "預估季均值": 預估季均值,
        "模型名稱": 模型結果["模型名稱"],
        "採用指標": 模型結果["採用指標"],
        "指標說明": 模型結果["指標說明"],
        "誤差率": 模型結果["誤差率"],
        "較無腦模型改善率": 模型結果["較無腦模型改善率"],
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測工作日數,
        "最佳訓練資料數": 模型結果["最佳訓練資料數"],
        "趨勢": 模型參數字典.b0,
        "近期影響權重": 模型參數字典.alpha,
        "最近歷史值時間": 最近日期,
        "最後預估值時間": 未來工作日時軸[-1],
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })

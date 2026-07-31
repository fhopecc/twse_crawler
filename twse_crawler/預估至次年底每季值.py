def 取預估至次年底每季值模型(
    歷季數值: "pd.Series",
    回測季數: int = 4,
    n_trials: int = 40
) -> "pd.Series":
    """
    一、優化目標：利用 Optuna 動態搜尋最佳「歷史記憶視窗大小」、「模型種類 (Theta vs Holt-Winters)」
               與最佳「評估指標 (wmpe 或 swmpe)」。
    二、模型擬合：動態支援 ThetaModel 與 ExponentialSmoothing (Holt-Winters)。
    三、基準比較：輸出最佳模型與 Naïve / Seasonal Naïve 模型的誤差率及改善率。
    四、回傳格式：pd.Series。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    from statsmodels.tsa.forecasting.theta import ThetaModel

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 2. 數據型態檢查、防禦與自動對齊 (相容 Q-DEC 等不同頻率標記)
    是季度索引 = isinstance(歷季數值.index, pd.PeriodIndex) and 歷季數值.index.freqstr.startswith('Q')
    
    if not 是季度索引:
        try:
            歷季數值.index = pd.to_datetime(歷季數值.index).to_period('Q')
        except Exception as e:
            raise ValueError(
                f"歷季數值索引無法轉換為季度型態，實際提供資料索引為 {type(歷季數值.index)}、"
                f"{getattr(歷季數值.index, 'freqstr', '無頻率')}，錯誤原因: {e}"
            )
        
    y_原始 = 歷季數值.astype(float)
    y_原始.index = y_原始.index.asfreq('Q')

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
        min_window = 8  # Theta 模型對視窗長度適應力較強，最低許容 8 季 (2 年)
        max_window = len(y_原始) - 回測季數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        # 🌟 動態選擇模型類型與評估指標
        model_type = trial.suggest_categorical('model_type', ['theta', 'holt_winters'])
        metric_choice = trial.suggest_categorical('metric', ['wmpe', 'swmpe'])

        單步回測值 = []
        有效回測步數 = 0
        
        # 執行 4 季歷史滾動盲測 (Walk-forward Validation)
        for i in range(回測季數):
            當前終點 = len(y_原始) - 回測季數 + i
            訓練起點 = max(0, 當前終點 - window_size)
            y_訓練 = y_原始.iloc[訓練起點:當前終點]
            
            try:
                if model_type == 'theta':
                    # Theta 模型（預設季節週期為 4 季，自動進行季節性檢定與平滑）
                    tm = ThetaModel(y_訓練, period=4, deseasonalize=True)
                    模型擬合 = tm.fit()
                    單季預測結果 = 模型擬合.forecast(1)
                    預測值 = 單季預測結果.values[0]
                else:
                    # Holt-Winters 季度指數平滑模型
                    模型 = ExponentialSmoothing(
                        y_訓練, 
                        trend='add', 
                        seasonal='add', 
                        seasonal_periods=4,
                        initialization_method='estimated'
                    )
                    模型擬合 = 模型.fit()
                    單季預測結果 = 模型擬合.forecast(1)
                    預測值 = 單季預測結果.values[0]
                
                if pd.isna(預測值) or np.isinf(預測值):
                    return float('inf')
                    
                單步回測值.append(預測值)
                有效回測步數 += 1
            except:
                return float('inf')
                
        if 有效回測步數 < 回測季數 or len(單步回測值) != 回測季數:
            return float('inf')

        回測值_陣列 = np.array(單步回測值)
        if not np.isfinite(回測值_陣列).all():
            return float('inf')

        真實值 = y_原始.iloc[-回測季數:].values
        snaive_base = y_原始.iloc[-回測季數-4:-4].values if len(y_原始) >= 回測季數 + 4 else 真實值
        
        當前_wmpe = calc_wmpe(真實值, 回測值_陣列)
        當前_swmpe = calc_swmpe(真實值, 回測值_陣列, snaive_base)
        
        trial.set_user_attr("wmpe", float(當前_wmpe))
        trial.set_user_attr("swmpe", float(當前_swmpe))
        
        if metric_choice == 'wmpe':
            return 當前_wmpe
        else:
            return 當前_swmpe

    # 5. 執行 Optuna 搜尋並獲取最優參數組合
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=n_trials)
    
    最佳參數 = 研究工廠.best_params
    最佳視窗 = 最佳參數.get('window_size', len(y_原始) - 回測季數)
    勝出模型 = 最佳參數.get('model_type', 'theta')
    勝出指標 = 最佳參數.get('metric', 'swmpe')

    最佳實驗 = 研究工廠.best_trial
    best_wmpe = 最佳實驗.user_attrs.get("wmpe", np.nan)
    best_swmpe = 最佳實驗.user_attrs.get("swmpe", np.nan)

    # 6. 計算 Naïve 與 Seasonal Naïve 改善率
    真實值 = y_原始.iloc[-回測季數:].values
    snaive_base = y_原始.iloc[-回測季數-4:-4].values if len(y_原始) >= 回測季數 + 4 else 真實值
    
    naive_pred = y_原始.iloc[-回測季數-1:-1].values
    naive_wmpe = calc_wmpe(真實值, naive_pred)
    naive_swmpe = calc_swmpe(真實值, naive_pred, snaive_base)

    snaive_pred = snaive_base
    snaive_wmpe = calc_wmpe(真實值, snaive_pred)
    snaive_swmpe = calc_swmpe(真實值, snaive_pred, snaive_base)

    naive_wmpe_改善率 = (naive_wmpe - best_wmpe) / naive_wmpe if naive_wmpe > 0 else np.nan
    snaive_swmpe_改善率 = (snaive_swmpe - best_swmpe) / snaive_swmpe if snaive_swmpe > 0 else np.nan

    # 7. 擬合最終勝出模型
    最終訓練起點 = max(0, len(y_原始) - 最佳視窗)
    y_最終訓練 = y_原始.iloc[最終訓練起點:]
    
    if 勝出模型 == 'theta':
        tm = ThetaModel(y_最終訓練, period=4, deseasonalize=True)
        最終模型擬合 = tm.fit()
        模型顯示名稱 = "Theta Model"
    else:
        最終模型 = ExponentialSmoothing(
            y_最終訓練, 
            trend='add', 
            seasonal='add', 
            seasonal_periods=4,
            initialization_method='estimated'
        )
        最終模型擬合 = 最終模型.fit()
        模型顯示名稱 = "Holt-Winters Exponential Smoothing"

    指標說明 = f"由 Optuna 自動盲測，勝出模型為 [{模型顯示名稱}]，評估指標採用 [{勝出指標.upper()}]"

    # 回傳型態維持為 pd.Series
    return pd.Series({
        "模型擬合": 最終模型擬合,
        "y_原始": y_原始,
        "y_最終訓練": y_最終訓練,
        "採用模型": 勝出模型,
        "模型名稱": 模型顯示名稱,
        "採用指標": 勝出指標,
        "指標說明": 指標說明,
        "wmpe": best_wmpe,
        "swmpe": best_swmpe,
        "naive_wmpe": naive_wmpe,
        "naive_swmpe": naive_swmpe,
        "snaive_wmpe": snaive_wmpe,
        "snaive_swmpe": snaive_swmpe,
        "naive_wmpe_改善率": naive_wmpe_改善率,
        "snaive_swmpe_改善率": snaive_swmpe_改善率,
        "最佳視窗": 最佳視窗,
        "回測季數": 回測季數
    })


def 預估至次年底每季值(
    歷季數值: "pd.Series",
    單位 = '元'
) -> "pd.Series":
    """
    一、預測結果：預估各季值至次年底。
    二、核心邏輯：調用「取預估至次年底每季值模型」獲取最佳模型 Series。
    三、防禦外推：全方位加入空值與發散值補強，適合業外損益與淨利等高波動財務科目。
    """
    # 1. 於函式內部進行套件導入
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    # 2. 調用專屬模型尋優函式取得模型 Series
    模型結果 = 取預估至次年底每季值模型(歷季數值)
    
    最終模型擬合 = 模型結果["模型擬合"]
    y_原始 = 模型結果["y_原始"]
    y_最終訓練 = 模型結果["y_最終訓練"]
    回測季數 = 模型結果["回測季數"]
    勝出模型 = 模型結果["採用模型"]

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底）
    最近季度 = y_原始.index[-1]
    次年數 = 今年數 + 1
    
    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')
    外推步數 = len(未來季時軸)

    # 4. 外推未來預測值至次年底
    預估季_陣列 = 最終模型擬合.forecast(外推步數)
    
    # 財務保守原則：防禦未來預測值發散 (NaN / Inf)，採用最後 4 季平均值安全填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_最終訓練.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估季_序列 = pd.Series(預估季_陣列.values, index=未來季時軸)

    # 5. 整合與計算各項延伸統計指標
    預估各季全序列 = pd.concat([y_原始, 預估季_序列])
    
    # 提取模型參數（適應 Theta 與 HW 不同的參數結構）
    if 勝出模型 == 'theta':
        模型參數字典 = {
            "b0": getattr(最終模型擬合, 'b0', None),
            "alpha": getattr(最終模型擬合, 'alpha', None)
        }
    else:
        模型參數字典 = {k: v for k, v in 最終模型擬合.params.items() if not k.startswith('_') and v is not None}
    
    最近歷史值同比 = (
        (y_原始.iloc[-1] - y_原始.iloc[-5]) / y_原始.iloc[-5] 
        if len(y_原始) > 4 and y_原始.iloc[-5] != 0 else np.nan
    )
    
    首期預估值同比 = (
        (預估季_序列.iloc[0] - y_原始.iloc[-4]) / y_原始.iloc[-4] 
        if len(y_原始) >= 3 and y_原始.iloc[-4] != 0 else np.nan
    )

    # 6. 回傳最終彙整 Series
    return pd.Series({
        "預估各季值": 預估各季全序列,
        "採用模型": 勝出模型,
        "模型名稱": 模型結果["模型名稱"],
        "採用指標": 模型結果["採用指標"],
        "指標說明": 模型結果["指標說明"],
        "wmpe": 模型結果["wmpe"],
        "swmpe": 模型結果["swmpe"],
        "naive_wmpe": 模型結果["naive_wmpe"],
        "naive_swmpe": 模型結果["naive_swmpe"],
        "snaive_wmpe": 模型結果["snaive_wmpe"],
        "snaive_swmpe": 模型結果["snaive_swmpe"],
        "naive_wmpe_改善率": 模型結果["naive_wmpe_改善率"],
        "snaive_swmpe_改善率": 模型結果["snaive_swmpe_改善率"],
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測季數,
        "模型參數": 模型參數字典,
        "最近歷史值時間": 最近季度,          # 保持 pd.Period 格式
        "最後預估值時間": 次年底最後一季,    # 保持 pd.Period 格式
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })

def 取預估至次年度每日值模型(歷日數值: "pd.Series") -> "pd.Series":
    """
    一、主要欄位：模型擬合、模型名稱、採用指標、誤差率、較無腦模型改善率、
                 最佳訓練資料數、回測資料數。
    二、輔助欄位：指標說明、wmpe、swmpe、naive_wmpe、naive_swmpe、
                 snaive_wmpe、snaive_swmpe、_y_原始、_y_最終訓練。
    三、最佳模型：回測 30 個工作日評估指標最小之 Theta 模型。若 Theta 表現不如無腦模型，
        則建立 NaiveFitResult 擬合物件並回傳，避免傳回較差結果[cite: 1, 2]。
    四、搜尋次數：固定執行 30 次 Optuna 試驗，兼顧搜尋品質與速度[cite: 2]。
    """
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.forecasting.theta import ThetaModel

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    class NaiveFitResult:
        """無腦模型（Naive / SNaive）擬合物件，供統一的 forecast 與 params 讀取[cite: 1]"""
        def __init__(self, mode: str, y_train: pd.Series):
            self.mode = mode
            self.last_value = y_train.iloc[-1]
            self.last_period_values = y_train.iloc[-252:].values if len(y_train) >= 252 else y_train.values

            class Params:
                pass
            self.params = Params()
            if mode == 'naive':
                self.params.b0 = 0.0
                self.params.alpha = 1.0
            else:
                self.params.b0 = 0.0
                self.params.alpha = 0.0

        def forecast(self, steps: int) -> pd.Series:
            if self.mode == 'naive':
                vals = np.full(steps, self.last_value)
            elif self.mode == 'snaive':
                cycles = (steps + 251) // 252
                vals = np.tile(self.last_period_values, cycles)[:steps]
            else:
                vals = np.full(steps, self.last_value)
            return pd.Series(vals)

    回測工作日數 = 30
    N_TRIALS = 30
    SEASON_PERIOD = 252

    try:
        y_tmp = 歷日數值.copy()
        y_tmp.index = pd.to_datetime(y_tmp.index).to_period('B')
    except Exception as e:
        raise ValueError(
            f"歷日數值索引無法轉換為工作日型態，實際提供資料索引為 {type(歷日數值.index)}，錯誤原因: {e}"
        )
    
    y_tmp = y_tmp[~y_tmp.index.duplicated(keep='last')]
    完整工作日時間軸 = pd.period_range(start=y_tmp.index[0], end=y_tmp.index[-1], freq='B')
    
    y_原始 = (
        y_tmp.reindex(完整工作日時間軸)
        .ffill()
        .bfill()
        .astype(float)
    )

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

    def objective(trial):
        min_window = 252
        max_window = len(y_原始) - 回測工作日數
        if max_window <= min_window:
            window_size = max(30, max_window)
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        deseasonalize_choice = trial.suggest_categorical('deseasonalize', [True, False])
        metric_choice = trial.suggest_categorical('metric', ['wmpe', 'swmpe'])

        訓練起點 = max(0, len(y_原始) - 回測工作日數 - window_size)
        訓練終點 = len(y_原始) - 回測工作日數
        y_訓練 = y_原始.iloc[訓練起點:訓練終點]
        y_真實驗證 = y_原始.iloc[訓練終點:].values  
        
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

    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=N_TRIALS)
    
    最佳參數 = 研究工廠.best_params
    最佳訓練資料數 = 最佳參數.get('window_size', len(y_原始) - 回測工作日數)
    勝出季節性 = 最佳參數.get('deseasonalize', True)
    勝出指標 = 最佳參數.get('metric', 'swmpe')

    最佳實驗 = 研究工廠.best_trial
    best_wmpe = 最佳實驗.user_attrs.get("wmpe", np.nan)
    best_swmpe = 最佳實驗.user_attrs.get("swmpe", np.nan)
    theta_best_error = best_wmpe if 勝出指標 == 'wmpe' else best_swmpe

    真實值 = y_原始.iloc[-回測工作日數:].values
    snaive_base = y_原始.iloc[-回測工作日數-SEASON_PERIOD:-SEASON_PERIOD].values if len(y_原始) >= 回測工作日數 + SEASON_PERIOD else 真實值
    
    naive_pred = np.full(回測工作日數, y_原始.iloc[-回測工作日數-1])
    naive_wmpe = calc_wmpe(真實值, naive_pred)
    naive_swmpe = calc_swmpe(真實值, naive_pred, snaive_base)

    snaive_pred = snaive_base
    snaive_wmpe = calc_wmpe(真實值, snaive_pred)
    snaive_swmpe = calc_swmpe(真實值, snaive_pred, snaive_base)

    無腦基線最優誤差 = min(naive_wmpe, naive_swmpe, snaive_wmpe, snaive_swmpe)

    if theta_best_error < 無腦基線最優誤差:
        誤差率 = theta_best_error
        最終訓練起點 = max(0, len(y_原始) - 最佳訓練資料數)
        y_最終訓練 = y_原始.iloc[最終訓練起點:]
        
        tm = ThetaModel(y_最終訓練, period=SEASON_PERIOD, deseasonalize=勝出季節性)
        最終模型擬合 = tm.fit()
        模型顯示名稱 = f"最小 {勝出指標.upper()} 之{'季節性' if 勝出季節性 else ''} Theta"
    else:
        最佳訓練資料數 = len(y_原始) - 回測工作日數
        y_最終訓練 = y_原始

        if min(naive_wmpe, naive_swmpe) <= min(snaive_wmpe, snaive_swmpe):
            模型顯示名稱 = "以上期值預測"
            誤差率 = min(naive_wmpe, naive_swmpe)
            最終模型擬合 = NaiveFitResult(mode='naive', y_train=y_最終訓練)
        else:
            模型顯示名稱 = "以去年同期值預測"
            誤差率 = min(snaive_wmpe, snaive_swmpe)
            最終模型擬合 = NaiveFitResult(mode='snaive', y_train=y_最終訓練)

    較無腦模型改善率 = (無腦基線最優誤差 - 誤差率) / 無腦基線最優誤差 if 無腦基線最優誤差 > 0 and not np.isinf(無腦基線最優誤差) else np.nan

    指標說明 = (
        f"由 Optuna 多步盲測（固定 {回測工作日數} 個工作日回測），訓練資料數為 {最佳訓練資料數} 個工作日，"
        f"採用 [{模型顯示名稱}]，評估指標為 [{勝出指標.upper()}]"
    )

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
        "_y_原始": y_原始,
        "_y_最終訓練": y_最終訓練
    })


def 預估至次年底工作日值丙式(
    歷日數值: "pd.Series",
    單位 = '元'
) -> "pd.Series":
    """
    一、預測結果：預估每日值、預估季均值、模型名稱、誤差率、歷史值數量、預估值數量、
                 回測資料數、最佳訓練資料數、趨勢、近期影響權重、最近歷史值時間、
                 最後預估值時間、最近歷史值同比、首期預估值同比。
    二、對齊每季值：統一返回與每季值相同的簡潔結構。
    """
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    模型結果 = 取預估至次年度每日值模型(歷日數值)
    最終模型擬合 = 模型結果["模型擬合"]
    模型名稱 = 模型結果["模型名稱"]
    y_原始 = 模型結果["_y_原始"]
    y_最終訓練 = 模型結果["_y_最終訓練"]
    回測工作日數 = 模型結果["回測資料數"]

    最近日期 = y_原始.index[-1]
    次年數 = 今年數 + 1
    
    未來下一工作日 = 最近日期 + 1
    次年底最後工作日 = pd.Period(f"{次年數}-12-31", freq='B')
    未來工作日時軸 = pd.period_range(start=未來下一工作日, end=次年底最後工作日, freq='B')
    外推步數 = len(未來工作日時軸)

    預估日_陣列 = 最終模型擬合.forecast(外推步數)
    
    if 預估日_陣列.isna().any() or np.isinf(預估日_陣列.values).any():
        安全填補值 = y_最終訓練.tail(252).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估日_陣列 = 預估日_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估日_序列 = pd.Series(預估日_陣列.values, index=未來工作日時軸)
    預估每日全序列 = pd.concat([y_原始, 預估日_序列])

    if isinstance(最近日期, pd.Period):
        本季標籤 = 最近日期.asfreq('Q')
    else:
        本季標籤 = pd.Period(pd.to_datetime(最近日期), freq='Q')

    if isinstance(預估每日全序列.index, pd.PeriodIndex):
        季分組索引 = 預估每日全序列.index.asfreq('Q')
    else:
        季分組索引 = pd.to_datetime(預估每日全序列.index).dt.to_period('Q')

    全序列季分組 = 預估每日全序列.groupby(季分組索引)

    全季均值 = 全序列季分組.mean()
    全季起日值 = 全序列季分組.first()
    全季迄日值 = 全序列季分組.last()
    全季增減數 = 全季迄日值 - 全季起日值

    預估季均值 = pd.concat([全季均值, 全季起日值, 全季迄日值, 全季增減數],
                          keys=["季均值", "季起日值", "季迄日值", "季增減數"],
                          axis=1)

    趨勢 = getattr(最終模型擬合.params, 'b0', 0.0)
    近期影響權重 = getattr(最終模型擬合.params, 'alpha', 0.0)

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
        "模型名稱": 模型名稱,
        "誤差率": 模型結果["誤差率"],
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測工作日數,
        "最佳訓練資料數": 模型結果["最佳訓練資料數"],
        "趨勢": 趨勢,
        "近期影響權重": 近期影響權重,
        "最近歷史值時間": 最近日期,
        "最後預估值時間": 未來工作日時軸[-1],
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比,
    })

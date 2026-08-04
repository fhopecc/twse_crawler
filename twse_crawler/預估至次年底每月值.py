def 取預估至次年底每月值模型(
    歷月數值: "pd.Series"
) -> "pd.Series":
    """
    一、主要欄位：模型擬合、模型名稱、採用指標、誤差率、較無腦模型改善率、
                 最佳訓練資料數、回測資料數。
    二、輔助欄位：指標說明、wmape、naive_wmape、snaive_wmape、_y_原始、_y_最終訓練。
    三、最佳模型：係回測 12 個月之 WMAPE 最小 Theta 之訓練資料數及是否去季節情形。
                 若最佳 Theta 表現劣於無腦模型 (改善率 < 0)，則自動切換退回採用最佳無腦模型。
    四、模型名稱：以去年同期值預測、以上期值預測
    五、搜尋次數：固定執行 30 次 Optuna 試驗，兼顧搜尋品質與速度。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.forecasting.theta import ThetaModel

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 內部固定參數
    回測月數 = 12
    N_TRIALS = 30
    勝出指標 = 'wmape'

    # 2. 數據型態檢查、防禦與自動對齊 (相容月度標記)
    是月度索引 = isinstance(歷月數值.index, pd.PeriodIndex) and 歷月數值.index.freqstr.startswith('M')
    if not 是月度索引:
        try:
            歷月數值.index = pd.to_datetime(歷月數值.index).to_period('M')
        except Exception as e:
            raise ValueError(
                f"歷月數值索引無法轉換為月度型態，實際提供資料索引為 {type(歷月數值.index)}、"
                f"{getattr(歷月數值.index, 'freqstr', '無頻率')}，錯誤原因: {e}"
            )
    y_原始 = 歷月數值.astype(float)
    y_原始.index = y_原始.index.asfreq('M')

    from twse_crawler.無腦預測至次年底每季值 import calc_wmape

    # 4. 定義 Optuna 最佳化目標函數
    def objective(trial):
        min_window = 24  # 最少 24 個月 (2 年)
        max_window = len(y_原始) - 回測月數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        deseasonalize_choice = trial.suggest_categorical('deseasonalize', [True, False])

        # 切分訓練集與驗證集
        訓練起點 = max(0, len(y_原始) - 回測月數 - window_size)
        訓練終點 = len(y_原始) - 回測月數
        y_訓練 = y_原始.iloc[訓練起點:訓練終點]
        y_真實驗證 = y_原始.iloc[訓練終點:].values  # 未來 12 個月真實值
        
        try:
            tm = ThetaModel(y_訓練, period=12, deseasonalize=deseasonalize_choice)
            模型擬合 = tm.fit()
            預測陣列 = 模型擬合.forecast(回測月數).values
            
            if pd.isna(預測陣列).any() or np.isinf(預測陣列).any() or len(預測陣列) != 回測月數:
                return float('inf')
        except:
            return float('inf')
        
        當前_wmape = calc_wmape(y_真實驗證, 預測陣列)
        trial.set_user_attr("wmape", float(當前_wmape))
        
        return 當前_wmape

    # 5. 執行 Optuna 搜尋
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=N_TRIALS)
    
    最佳參數 = 研究工廠.best_params
    最佳訓練資料數 = 最佳參數.get('window_size', len(y_原始) - 回測月數)
    勝出季節性 = 最佳參數.get('deseasonalize', True)

    最佳實驗 = 研究工廠.best_trial
    best_wmape = 最佳實驗.user_attrs.get("wmape", np.nan)

    # 6. 計算 Naïve 與 Seasonal Naïve 基準的 WMAPE
    真實值 = y_原始.iloc[-回測月數:].values
    
    # Naïve 基準 (拿回測點前最後一期平移)
    naive_pred = np.full(回測月數, y_原始.iloc[-回測月數-1])
    naive_wmape = calc_wmape(真實值, naive_pred)

    # Seasonal Naïve 基準 (拿前一年同期值)
    snaive_base = y_原始.iloc[-回測月數-12:-12].values if len(y_原始) >= 回測月數 + 12 else 真實值
    snaive_pred = snaive_base
    snaive_wmape = calc_wmape(真實值, snaive_pred)

    # 7. 模型評比與退回無腦模型判定
    模型最佳誤差 = best_wmape

    # 選出最優無腦基準
    if snaive_wmape < naive_wmape:
        無腦基線最優誤差 = snaive_wmape
        最佳無腦類型 = '以去年同期值預測'
    else:
        無腦基線最優誤差 = naive_wmape
        最佳無腦類型 = '以上期值預測'

    # 計算改善率
    if 無腦基線最優誤差 > 0 and not np.isinf(無腦基線最優誤差):
        較無腦模型改善率 = (無腦基線最優誤差 - 模型最佳誤差) / 無腦基線最優誤差
    else:
        較無腦模型改善率 = np.nan

    # 8. 構建最終模型（若 Theta 表現比無腦模型差，即改善率 < 0 或改善率為 NaN，則退回無腦模型）
    採用無腦模型 = (pd.isna(較無腦模型改善率) or 較無腦模型改善率 < 0)

    if 採用無腦模型:
        最終訓練起點 = 0
        y_最終訓練 = y_原始
        誤差率 = 無腦基線最優誤差
        模型顯示名稱 = 最佳無腦類型
        
        # 封裝與 Statsmodels 介面相容的 Dummy 擬合物件
        class NaiveModelFit:
            def __init__(self, mode, y_data):
                self.mode = mode
                self.y_data = y_data
                self.params = pd.Series({'b0': 0.0, 'alpha': 0.0})

            def forecast(self, steps):
                if self.mode == '以上期值預測':
                    last_val = self.y_data.iloc[-1]
                    return pd.Series(np.full(steps, last_val))
                else:  # 以去年同期值預測
                    last_12 = self.y_data.iloc[-12:].values
                    reps = int(np.ceil(steps / 12))
                    forecast_vals = np.tile(last_12, reps)[:steps]
                    return pd.Series(forecast_vals)

        最終模型擬合 = NaiveModelFit(最佳無腦類型, y_原始)

    else:
        最終訓練起點 = max(0, len(y_原始) - 最佳訓練資料數)
        y_最終訓練 = y_原始.iloc[最終訓練起點:]
        誤差率 = 模型最佳誤差
        模型顯示名稱 = f"Theta"

        tm = ThetaModel(y_最終訓練, period=12, deseasonalize=勝出季節性)
        最終模型擬合 = tm.fit()

    指標說明 = (
        f"由 Optuna 多步盲測（固定 12 個月回測），採用 [{模型顯示名稱}]，"
        f"評估指標為 [WMAPE]，最佳訓練資料數為 {最佳訓練資料數} 個月"
    )

    _y_原始 = y_原始
    _y_最終訓練 = y_最終訓練

    return pd.Series({
        "模型擬合": 最終模型擬合,
        "模型名稱": 模型顯示名稱,
        "採用指標": 勝出指標,
        "指標說明": 指標說明,
        "誤差率": 誤差率,
        "wmape": best_wmape,
        "naive_wmape": naive_wmape,
        "snaive_wmape": snaive_wmape,
        "較無腦模型改善率": 較無腦模型改善率,
        "最佳訓練資料數": 最佳訓練資料數,
        "回測資料數": 回測月數,
        "_y_原始": _y_原始,
        "_y_最終訓練": _y_最終訓練
    })


def 預估至次年底每月值丙式(
    歷月數值: "pd.Series"
) -> "pd.Series":
    """
    一、預測結果：預估各月值、最近歷史值同比、首期預估值同比、誤差率、較無腦模型改善率、
                 趨勢、近期影響權重。
    二、預測季結果：預估每季總值。
    三、預測方法：模型名稱、採用指標、指標說明、歷史值數量、預估值數量、回測資料數、
                 最佳訓練資料數、模型參數、最近歷史值時間、最後預估值時間。
    四、外推防禦：加入空值與發散值補強，適合業外損益與淨利等高波動財務科目。
    """
    # 1. 於函式內部進行套件導入
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    # 2. 調用專屬模型尋優函式（僅傳入歷月數值）
    模型結果 = 取預估至次年底每月值模型(歷月數值)
    最終模型擬合 = 模型結果["模型擬合"]
    y_原始 = 模型結果["_y_原始"]
    y_最終訓練 = 模型結果["_y_最終訓練"]
    回測月數 = 模型結果["回測資料數"]

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底）
    最近月份 = y_原始.index[-1]
    次年數 = 今年數 + 1
    
    未來下一月 = 最近月份 + 1
    次年底最後一個月 = pd.Period(f"{次年數}-12", freq='M')
    未來月時軸 = pd.period_range(start=未來下一月, end=次年底最後一個月, freq='M')
    外推步數 = len(未來月時軸)

    # 4. 外推未來預測值至次年底
    預估月_陣列 = 最終模型擬合.forecast(外推步數)
    
    # 財務保守原則：防禦未來預測值發散 (NaN / Inf)，採最後 12 個月平均值安全填充
    if pd.isna(預估月_陣列).any() or np.isinf(預估月_陣列.values).any():
        安全填補值 = y_最終訓練.tail(12).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估月_陣列 = 預估月_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估月_序列 = pd.Series(預估月_陣列.values, index=未來月時軸)

    # 5. 整合與計算各項延伸統計指標
    預估各月全序列 = pd.concat([y_原始, 預估月_序列])
    
    # 安全獲取模型參數 (若退回至無腦模型，b0 與 alpha 皆為 0.0 或 np.nan)
    params = getattr(最終模型擬合, 'params', None)
    b0 = params.b0 if params is not None and hasattr(params, 'b0') else np.nan
    alpha = params.alpha if params is not None and hasattr(params, 'alpha') else np.nan

    模型參數字典 = pd.Series({
        "b0": b0,
        "alpha": alpha
    })
    
    # 同比為與上年同月相比 (相差 12 個月)
    最近歷史值同比 = (
        (y_原始.iloc[-1] - y_原始.iloc[-13]) / y_原始.iloc[-13] 
        if len(y_原始) > 12 and y_原始.iloc[-13] != 0 else np.nan
    )
    
    首期預估值同比 = (
        (預估月_序列.iloc[0] - y_原始.iloc[-12]) / y_原始.iloc[-12] 
        if len(y_原始) >= 12 and y_原始.iloc[-12] != 0 else np.nan
    )

    預估每季總值 = 預估各月全序列.resample('Q').sum()

    # 6. 回傳最終彙整 Series
    return pd.Series({
        "預估各月值": 預估各月全序列,
        "預估每季總值": 預估每季總值,
        "模型名稱": 模型結果["模型名稱"],
        "指標說明": 模型結果["指標說明"],
        "誤差率": 模型結果["誤差率"],
        "較無腦模型改善率": 模型結果["較無腦模型改善率"],
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測月數,
        "最佳訓練資料數": 模型結果["最佳訓練資料數"],
        "趨勢": 模型參數字典.b0,
        "近期影響權重": 模型參數字典.alpha,
        "最近歷史值時間": 最近月份,
        "最後預估值時間": 次年底最後一個月,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })

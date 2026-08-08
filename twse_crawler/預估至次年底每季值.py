def 表達預估說明丙(預估結果, 預估目標='淨利', 時間單位='季'):
    from zhongwen.數 import 取增減百分比
    from twse_crawler.預估次年底 import 表達期間

    if 'Theta' in 預估結果.模型名稱:
        if '季節' in 預估結果.模型名稱:
            模型個別預測項目 = f'，具季節性且趨勢{"向上" if 預估結果.趨勢 > 0 else "向下"}'
        else:
            模型個別預測項目 = f'，趨勢{"向上" if 預估結果.趨勢 > 0 else "向下"}'
        模型個別預測項目 += f'，近期影響占{預估結果.近期影響權重:,.2%}'
    else:
        模型個別預測項目 = ''

    if '以去年同期值預測' in 預估結果.模型名稱:
        預估次期同比 = '以去年同期值預測'
    else:
        預估次期同比 = f'預估次{時間單位}同比{取增減百分比(預估結果.首期預估值同比)}{模型個別預測項目}'

    return f'{表達期間(預估結果.最近歷史值時間)}{預估目標}同比{取增減百分比(預估結果.最近歷史值同比)}，{預估次期同比}，**誤差{預估結果.誤差率:,.0%}**'


def 表達預估方法丙(預估結果, 預估目標='淨利', 單位='元', 時間單位='季'):
    from twse_crawler.預估次年底 import 表達期間
    from zhongwen.數 import 取最簡約數

    m = 預估結果
    訓練資料說明 = f'依{表達期間(m.最近歷史值時間)}前{m.歷史值數量:,}{時間單位}{預估目標}'
    訓練資料說明 += f'回測{m.回測資料數}{時間單位}'
    訓練資料說明 += f'得以過去{m.最佳訓練資料數}{時間單位}{預估目標}訓練之'
    預估數量說明 = f'預估至{m.最後預估值時間.year-1911:,}年底之{m.預估值數量:,}{時間單位}{預估目標}'
    return (
        f'{訓練資料說明}'
        f'{m.模型名稱}，{預估數量說明}'
    )


def 取預估至次年底每季值模型(歷季數值: "pd.Series") -> "pd.Series":
    """
    一、主要欄位：模型擬合、模型名稱、指標說明、誤差率、最佳訓練資料數、回測資料數。
    二、輔助欄位：wmape、y、y_best_train。
    三、最佳模型：回測 4 季評估指標最小之 Theta 模型。若 Theta 表現不如無腦模型，
        則建立 NaiveFitResult 擬合物件並回傳，避免傳回 None。
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

    # 內部鴨子型態擬合物件：模擬 statsmodels fit 物件介面
    class NaiveFitResult:
        """無腦模型（Naive / SNaive）擬合物件，供統一的 forecast 與 params 讀取"""
        def __init__(self, mode: str, y_train: pd.Series):
            self.mode = mode  # 'naive' 或 'snaive'
            self.last_value = y_train.iloc[-1]
            self.last_4_values = y_train.iloc[-4:].values if len(y_train) >= 4 else y_train.values

            class Params:
                pass
            self.params = Params()
            if mode == 'naive':
                self.params.b0 = 0.0
                self.params.alpha = 1.0
            else:  # snaive
                self.params.b0 = 0.0
                self.params.alpha = 0.0

        def forecast(self, steps: int) -> pd.Series:
            if self.mode == 'naive':
                vals = np.full(steps, self.last_value)
            elif self.mode == 'snaive':
                cycles = (steps + 3) // 4
                vals = np.tile(self.last_4_values, cycles)[:steps]
            else:
                vals = np.full(steps, self.last_value)
            return pd.Series(vals)

    # 內部固定參數
    回測季數 = 4
    N_TRIALS = 30

    # 2. 數據型態檢查、防禦與自動對齊
    是季度索引 = isinstance(歷季數值.index, pd.PeriodIndex) and 歷季數值.index.freqstr.startswith('Q')
    if not 是季度索引:
        try:
            歷季數值.index = pd.to_datetime(歷季數值.index).to_period('Q')
        except Exception as e:
            raise ValueError(
                f"歷季數值索引無法轉換為季度型態，實際提供資料索引為 {type(歷季數值.index)}、"
                f"{getattr(歷季數值.index, 'freqstr', '無頻率')}，錯誤原因: {e}"
            )

    y = 歷季數值.astype(float)
    y.index = y.index.asfreq('Q')

    from twse_crawler.無腦預測至次年底每季值 import calc_wmape

    # 3. 定義 Optuna 最佳化目標函數
    def objective(trial):
        min_window = 8  # 最少 8 季 (2 年)
        max_window = len(y) - 回測季數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        deseasonalize_choice = trial.suggest_categorical('deseasonalize', [True, False])

        訓練起點 = max(0, len(y) - 回測季數 - window_size)
        訓練終點 = len(y) - 回測季數
        y_train = y.iloc[訓練起點:訓練終點]
        y_validation = y.iloc[訓練終點:].values

        try:
            tm = ThetaModel(y_train, period=4, deseasonalize=deseasonalize_choice)
            模型擬合 = tm.fit()
            預測陣列 = 模型擬合.forecast(回測季數).values
            if pd.isna(預測陣列).any() or np.isinf(預測陣列).any() or len(預測陣列) != 回測季數:
                return float('inf')
        except Exception:
            return float('inf')

        當前_wmape = calc_wmape(y_validation, 預測陣列)
        trial.set_user_attr("wmape", float(當前_wmape))
        return 當前_wmape

    # 4. 執行 Optuna 搜尋
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=N_TRIALS)

    最佳參數 = 研究工廠.best_params
    最佳訓練資料數 = 最佳參數.get('window_size', len(y) - 回測季數)
    勝出季節性 = 最佳參數.get('deseasonalize', True)
    最佳實驗 = 研究工廠.best_trial
    best_wmape = 最佳實驗.user_attrs.get("wmape", np.nan)

    # 5. 直接評估較佳無腦模型並比較
    真實值 = y.iloc[-回測季數:].values
    snaive_base = y.iloc[-回測季數 - 4:-4].values if len(y) >= 回測季數 + 4 else 真實值
    naive_pred = np.full(回測季數, y.iloc[-回測季數 - 1])
    naive_wmape = calc_wmape(真實值, naive_pred)
    snaive_wmape = calc_wmape(真實值, snaive_base)
    無腦基線最優誤差 = min(naive_wmape, snaive_wmape)

    # 6. 判斷是否採納 Theta 模型或改採無腦模型
    if best_wmape < 無腦基線最優誤差:
        # Theta 模型勝出
        誤差率 = best_wmape
        最終訓練起點 = max(0, len(y) - 最佳訓練資料數)
        y_best_train = y.iloc[最終訓練起點:]
        tm = ThetaModel(y_best_train, period=4, deseasonalize=勝出季節性)
        最終模型擬合 = tm.fit()
        模型顯示名稱 = f"{'去季節性' if 勝出季節性 else ''}Theta"
    else:
        # 無腦模型勝出：建立 NaiveFitResult 擬合物件，確保回傳具備 forecast 介面的物件
        最佳訓練資料數 = len(y) - 回測季數
        y_best_train = y

        if naive_wmape <= snaive_wmape:
            模型顯示名稱 = "以上期值預測"
            誤差率 = naive_wmape
            最終模型擬合 = NaiveFitResult(mode='naive', y_train=y_best_train)
        else:
            模型顯示名稱 = "以去年同期值預測"
            誤差率 = snaive_wmape
            最終模型擬合 = NaiveFitResult(mode='snaive', y_train=y_best_train)

    指標說明 = (
        f"由 Optuna 多步盲測（固定 4 季回測），訓練資料數為 {最佳訓練資料數} 季，"
        f"採用 [{模型顯示名稱}]，評估指標為 [WMAPE]"
    )

    return pd.Series({
        "模型擬合": 最終模型擬合,
        "模型名稱": 模型顯示名稱,
        "指標說明": 指標說明,
        "誤差率": 誤差率,
        "wmape": 誤差率,
        "最佳訓練資料數": 最佳訓練資料數,
        "回測資料數": 回測季數,
        "_y": y,
        "_y_best_train": y_best_train,
    })


def 預估至次年底每季值丙式(歷季數值: "pd.Series") -> "pd.Series":
    """
    一、預測結果：預估各季值、最近歷史值同比、首期預估值同比、誤差率、
        趨勢、近期影響權重。
    二、預測方法：模型名稱、採用指標、指標說明、歷史值數量、預估值數量、回測資料數、
        最佳訓練資料數、模型參數、最近歷史值時間、最後預估值時間。
    三、外推防禦：加入空值與發散值補強，適合業外損益與淨利等高波動財務科目。
    """
    # 1. 於函式內部進行套件導入
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    # 2. 調用專屬模型尋優函式
    模型結果 = 取預估至次年底每季值模型(歷季數值)
    最終模型擬合 = 模型結果["模型擬合"]
    模型名稱 = 模型結果["模型名稱"]
    y = 模型結果["_y"]
    y_best_train = 模型結果["_y_best_train"]
    回測季數 = 模型結果["回測資料數"]

    # 3. 動態識別並建構預測時間軸
    最近季度 = y.index[-1]
    次年數 = 今年數 + 1
    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')
    外推步數 = len(未來季時軸)

    # 4. 統一使用 .forecast() 外推未來預測值與取得參數
    預估季_陣列 = 最終模型擬合.forecast(外推步數)
    預估季_陣列.index = 未來季時軸

    趨勢 = getattr(最終模型擬合.params, 'b0', 0.0)
    近期影響權重 = getattr(最終模型擬合.params, 'alpha', 0.0)

    # 財務保守原則：防禦未來預測值發散 (NaN / Inf)，採最後 4 季平均值安全填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_best_train.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)

    預估季_序列 = pd.Series(預估季_陣列.values, index=未來季時軸)

    # 5. 整合與計算各項延伸統計指標
    預估各季全序列 = pd.concat([y, 預估季_序列])
    最近歷史值同比 = (
        (y.iloc[-1] - y.iloc[-5]) / y.iloc[-5]
        if len(y) > 4 and y.iloc[-5] != 0
        else np.nan
    )
    首期預估值同比 = (
        (預估季_序列.iloc[0] - y.iloc[-4]) / y.iloc[-4]
        if len(y) >= 3 and y.iloc[-4] != 0
        else np.nan
    )

    # 6. 回傳最終彙整 Series
    return pd.Series({
        "預估各季值": 預估各季全序列,
        "模型名稱": 模型名稱,
        "誤差率": 模型結果["誤差率"],
        "歷史值數量": len(y),
        "預估值數量": 外推步數,
        "回測資料數": 回測季數,
        "最佳訓練資料數": 模型結果["最佳訓練資料數"],
        "趨勢": 趨勢,
        "近期影響權重": 近期影響權重,
        "最近歷史值時間": 最近季度,
        "最後預估值時間": 次年底最後一季,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比,
    })

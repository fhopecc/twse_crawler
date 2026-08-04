def calc_wmape(y_true, y_pred):
    import numpy as np
    sum_abs_true = np.sum(np.abs(y_true))
    if sum_abs_true == 0:
        return float('inf')
    return np.sum(np.abs(y_true - y_pred)) / sum_abs_true

def 取最佳無腦預估至次年底每季值模型(
    歷季數值: "pd.Series"
) -> "pd.Series":
    """
    一、主要欄位：勝出無腦模型類型、誤差率、回測資料數。
    二、模型名稱：以去年同期值預測、以上期值預測。
    二、輔助欄位：wmape、naive_wmape、snaive_wmape、_y_原始。
    三、最佳模型：盲測過去 4 季，比較上期外推 (Naïve) 與去年同期外推 (SNaïve) 之 WMAPE 最小誤差者。
    """
    import warnings
    import numpy as np
    import pandas as pd

    warnings.filterwarnings("ignore")

    # 內部固定參數
    回測季數 = 4

    # 2. 數據型態檢查與自動對齊
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

    # 4. 盲測過去 4 季（回測評估）
    y_真實驗證 = y_原始.iloc[-回測季數:].values
    y_訓練集 = y_原始.iloc[:-回測季數]

    # (1) 上期無腦預測 (Naïve)：用訓練集最後一期平鋪 4 季
    naive_pred = np.full(回測季數, y_訓練集.iloc[-1])
    
    # (2) 去年同期無腦預測 (SNaïve)：用訓練集倒數 4 季對應同期
    if len(y_訓練集) >= 4:
        snaive_pred = y_訓練集.iloc[-4:].values
    else:
        snaive_pred = naive_pred  # 資料不足時退回 Naïve

    # 計算各模型 WMAPE 誤差
    naive_wmape = calc_wmape(y_真實驗證, naive_pred)
    snaive_wmape = calc_wmape(y_真實驗證, snaive_pred)

    # 5. 比對 Naïve 與 SNaïve，選擇 WMAPE 最小者
    candidates = [
        ('naive', naive_wmape),
        ('snaive', snaive_wmape),
    ]
    # 按誤差由小到大排序，取最優者
    candidates.sort(key=lambda x: x[1])
    勝出模型類型, 誤差率 = candidates[0]

    模型顯示名稱 = f"{'以去年同期值預測' if 勝出模型類型 == 'snaive' else '以上期值預測'}"

    return pd.Series({
        "模型擬合": 勝出模型類型,  # 'naive' 或 'snaive'
        "模型名稱": 模型顯示名稱,
        "誤差率": 誤差率,
        "wmape": 誤差率,
        "naive_wmape": naive_wmape,
        "snaive_wmape": snaive_wmape,
        "回測資料數": 回測季數,
        "_y_原始": y_原始
    })


def 無腦預估至次年底每季值(
    歷季數值: "pd.Series",
) -> "pd.Series":
    """
    一、預測結果：預估各季值、最近歷史值同比、首期預估值同比、誤差率。
    二、預測方法：模型名稱、歷史值數量、預估值數量、回測資料數、
                 最近歷史值時間、最後預估值時間。
    三、外推防禦：以無腦外推（上期或去年同期）填補至次年底，加入空值與發散值補強。
    """
    # 1. 於函式內部進行套件導入
    import numpy as np
    import pandas as pd
    from zhongwen.時 import 今年數

    # 2. 調用專屬模型尋優函式（僅傳入歷季數值）
    模型結果 = 取最佳無腦預估至次年底每季值模型(歷季數值)
    勝出模型類型 = 模型結果["模型擬合"]
    y_原始 = 模型結果["_y_原始"]
    回測季數 = 模型結果["回測資料數"]

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底）
    最近季度 = y_原始.index[-1]
    次年數 = 今年數 + 1

    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')
    外推步數 = len(未來季時軸)

    # 4. 根據勝出的無腦模型邏輯進行外推預測
    if 勝出模型類型 == 'naive':
        # 上期無腦預測：直接複製歷史最後一季數值
        預估季_陣列 = np.full(外推步數, y_原始.iloc[-1])
    else:
        # 去年同期無腦預測：按季度循環（週期 4）抓取歷史同期值外推
        歷史長度 = len(y_原始)
        預估季_列表 = []
        for i in range(外推步數):
            idx = (i % 4) - 4
            if abs(idx) <= 歷史長度:
                預估季_列表.append(y_原始.iloc[idx])
            else:
                預估季_列表.append(y_原始.iloc[-1])
        預估季_陣列 = np.array(預估季_列表)

    預估季_序列 = pd.Series(預估季_陣列, index=未來季時軸)

    # 財務保守原則：防禦未來預測值發散 (NaN / Inf)，採最後 4 季平均值安全填充
    if 預估季_序列.isna().any() or np.isinf(預估季_序列.values).any():
        安全填補值 = y_原始.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_序列 = 預估季_序列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)

    # 5. 整合與計算各項延伸統計指標
    預估各季全序列 = pd.concat([y_原始, 預估季_序列])

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
        "模型名稱": 模型結果["模型名稱"],
        "誤差率": 模型結果["誤差率"],
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測季數,
        "最近歷史值時間": 最近季度,
        "最後預估值時間": 次年底最後一季,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })

def 表達預估方法(預估結果, 預估目標='業外損益', 單位='元', 時間單位='季'):
    from zhongwen.數 import 取最簡約數
    m = 預估結果
    訓練資料說明 = f'依{表達期間(m.最近歷史值時間)}前{m.歷史值數量:,}{時間單位}{預估目標}訓練'
    預估數量說明 = f'預估至{m.最後預估值時間.year-1911:,}年底之{m.預估值數量:,}{時間單位}{預估目標}'
    if 單位=='%':
        預估誤差範圍說明 = f'所得回測{m.回測資料數:,}{時間單位}之誤差範圍為{m.rmse:,.2%}'
    else:
        預估誤差範圍說明 = f'所得回測{m.回測資料數:,}{時間單位}之誤差範圍為{取最簡約數(m.rmse)}{單位}'
    模型參數 = ''
    if m.模型名稱 == '以 HAC 調整共變數之 OLS':
        try:
            滯後數=m.模型參數["lag_quarters"]
        except KeyError:
            滯後數=m.模型參數["lag_months"]
        if 滯後數>0:
            try:
                模型參數 = (f'，滯後{滯後數}{時間單位}之'
                            f'近{m.模型參數["window_size"]}{時間單位}{預估目標}訓練'
                            )
            except KeyError:
                模型參數 = f'，滯後{滯後數}{時間單位}之{預估目標}訓練'
        else:
            try:
                模型參數 = (f'，近{m.模型參數["window_size"]}{時間單位}{預估目標}訓練')
            except KeyError:
                模型參數 = f'{預估目標}訓練'
    elif m.模型名稱 == 'Theta':
        try:
            模型參數 = (f'，以近{m.模型參數["window_size"]}{時間單位}{預估目標}'
                        f'採用{m.模型參數["method"]}方法訓練'
                       )
        except KeyError:
            模型參數 = (f'採用{m.模型參數["method"]}方法訓練')

    return (
        f'{訓練資料說明}{預估誤差範圍說明}'
        f'，比率{m.mape:,.2%}{模型參數}'
        f'之{m.模型名稱}模型，{預估數量說明}'
    )

def 移除重覆時間詞(text):
    import re
    # 定義時間詞的正規表示式（匹配：幾年幾月、幾年第幾季、幾年、幾年度）
    # 例如：115年5月、115年第2季、114年度
    time_pattern = r"\d+年(?:第\d+季|\d+月|度)?"

    # 用來記錄已經出現過的時間詞
    seen_time_words = set()

    # 定義一個替換函數
    def replace_func(match):
        word = match.group(0)
        if word in seen_time_words:
            # 如果這個時間詞之前出現過，就返回空字串（刪除）
            return ""
        else:
            # 如果是第一次出現，記錄下來並保留
            seen_time_words.add(word)
            return word

    # re.sub 會逐一掃描文字，並把匹配到的時間詞丟進 replace_func 處理
    result = re.sub(time_pattern, replace_func, text)

    return result

def 表達期間(期間):
    import pandas as pd
    if isinstance(期間, pd.Period):
        freq = 期間.freqstr
        if freq.startswith('Q'):
            from zhongwen.時 import 取民國季度
            return 取民國季度(期間)
        elif freq.startswith('M'):
            from zhongwen.時 import 取民國月份
            return 取民國月份(期間)
    from zhongwen.時 import 取正式民國日期
    return 取正式民國日期(期間)

def 表達預估說明(預估結果, 預估目標='毛利率', 時間單位='季'):
    from zhongwen.數 import 取增減百分比
    from twse_crawler.預估次年底 import 表達期間
    return f'{表達期間(預估結果.最近歷史值時間)}{預估目標}同比{取增減百分比(預估結果.最近歷史值同比)}，預估次{時間單位}同比{取增減百分比(預估結果.首期預估值同比)}，誤差{預估結果.mape:,.0%}'

def 表達模型精準度(rmse, mape, 單位='%') -> str:
    from zhongwen.數 import 最簡約數
    if 單位=='%':
        m  = f'誤差範圍{rmse:,.2%}，'
    else:
        if rmse > 10**4:
            m  = f'誤差範圍{最簡約數(rmse)}{單位}，'
        else:
            m  = f'誤差範圍{rmse:,.0f}{單位}，'
    m += f'誤差率{mape:,.0%}'
    return m

def 預估次年底值(歷日值: "pd.Series", 預估目標 = '鉛價', 單位 = '美元'):
    """
    一、歷日值索引須是 DatetimeIndex。
    二、預估結果項目：預估值、預估季均值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                      模型名稱、模型參數、最近歷史值時間、最後預估值時間。
    三、預估季均值：季均值、季起日值、季迄日值、季增減數。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    from sklearn.metrics import mean_squared_error
    from statsmodels.tsa.forecasting.theta import ThetaModel

    warnings.filterwarnings("ignore")

    # 2. 檢查與常規化時間序列
    if not isinstance(歷日值.index, pd.DatetimeIndex):
        raise ValueError(f"歷日值索引必須為 DatetimeIndex，實際提供資料索引為 {type(歷日值.index)}")

    # 🛡️ 型態防禦：強制轉換為 float 浮點數，並補齊工作日
    歷日值 = 歷日值.asfreq("B").ffill().astype(float)
    歷史值數量 = len(歷日值)

    # 3. 檢查回測資料量
    回測天數 = 260
    if 歷史值數量 <= 回測天數 + 50:
        raise ValueError("歷日值資料太少，無法回測！")

    # =====================================================================
    # 🎯 定義核心回測評估函式 (計算特定參數在過去 260 天的模擬考分數)
    # =====================================================================
    def 評估參數組合(deseasonalize_type, method_type, theta_2):
        回測預測字典 = {}
        for i in range(回測天數):
            目標位置 = 歷史值數量 - 回測天數 + i
            回測訓練集 = 歷日值.iloc[:目標位置]
            預測目標日期 = 歷日值.index[目標位置]
            
            try:
                p_val = 260 if deseasonalize_type else None
                
                # 建立模型時只傳入結構參數
                回測模型 = ThetaModel(
                    回測訓練集, 
                    period=p_val, 
                    deseasonalize=deseasonalize_type, 
                    method=method_type
                ).fit()
                
                # 🛡️ 正確做法：將 theta 傳在 .forecast() 裡面，且只傳單一數值
                單日預測 = 回測模型.forecast(steps=1, theta=theta_2)
                回測預測字典[預測目標日期] = 單日預測.values[0]
            except:
                回測預測字典[預測目標日期] = np.nan
                
        回測值_Series = pd.Series(回測預測字典).dropna()
        
        # 🛡️ 高容錯防禦：放寬考核標準，只要成功預測大於 80% 的天數 (208天) 就允許評估
        if len(回測值_Series) < (回測天數 * 0.8):
            return float('inf'), float('inf')
            
        真實歷史值 = 歷日值.loc[回測值_Series.index]
        
        # 計算回測指標
        current_rmse = np.sqrt(mean_squared_error(真實歷史值, 回測值_Series))
        current_mape = np.mean(np.abs((真實歷史值 - 回測值_Series) / 真實歷史值))
        return current_rmse, current_mape

    # =====================================================================
    # 🔍 執行 Grid Search 網格搜索 (2 x 2 x 4 = 16 種組合地毯式搜索)
    # =====================================================================
    grid_space = {
        'deseasonalize': [True, False],               
        'method': ["linear", "exponential"],          
        'theta_2': [1.5, 2.0, 2.5, 3.0]               
    }

    最佳模型參數 = {}
    最佳_mape = float('inf')
    最佳_rmse = float('inf')

    # 開始地毯式搜索
    for d_type in grid_space['deseasonalize']:
        for meth_type in grid_space['method']:
            for t2 in grid_space['theta_2']:
                
                res_rmse, res_mape = 評估參數組合(d_type, meth_type, t2)
                
                if res_mape < 最佳_mape:
                    最佳_mape = res_mape
                    最佳_rmse = res_rmse
                    最佳模型參數 = {
                        "deseasonalize": d_type, 
                        "method": meth_type, 
                        "theta_2": t2          # 儲存單一 float 即可
                    }

    # =====================================================================
    # 🛡️ 安全降級機制 (Fallback)
    # =====================================================================
    if not 最佳模型參數:
        最佳模型參數 = {
            "deseasonalize": True,
            "method": "auto",
            "theta_2": 2.0
        }
        最佳_rmse, 最佳_mape = 評估參數組合(最佳模型參數["deseasonalize"], 最佳模型參數["method"], 最佳模型參數["theta_2"])
        模型名稱 = "Autotheta"
    else:
        模型名稱 = "Theta"

    # =====================================================================
    # 🔮 使用「最佳模型參數」建立最終模型並外推預測
    # =====================================================================
    最近日期 = 歷日值.index[-1]
    今年數 = 最近日期.year
    次年數 = 今年數 + 1
    次年底 = pd.Timestamp(f"{次年數}-12-31")

    未來時軸 = pd.date_range(start=最近日期 + pd.offsets.BDay(1), end=次年底, freq="B")
    預測日數 = len(未來時軸)
    最後預估日期 = 未來時軸[-1] if 預測日數 > 0 else 最近日期

    final_p_val = 260 if 最佳模型參數["deseasonalize"] else None
    
    最終模型 = ThetaModel(
        歷日值, 
        period=final_p_val, 
        deseasonalize=最佳模型參數["deseasonalize"], 
        method=最佳模型參數["method"]
    ).fit()
    
    # 🛡️ 最終預測同樣將 theta_2 傳入 forecast 中
    預估值_陣列 = 最終模型.forecast(steps=預測日數, theta=最佳模型參數["theta_2"])
    預估值 = pd.Series(預估值_陣列.values, index=未來時軸, name="預估值")

    # =====================================================================
    # 📈 將歷史與預估值結合，計算「預估季均值」
    # =====================================================================
    全時軸日值 = pd.concat([歷日值, 預估值])
    try:
        # 優先嘗試新版 Pandas 的 Quarter End 代號
        季聚合 = 全時軸日值.resample('QE')
    except ValueError:
        # 如果是舊版 Pandas 噴錯，則退回使用舊代號
        季聚合 = 全時軸日值.resample('Q')
    預估季均值 = pd.DataFrame({
        "季均值": 季聚合.mean(),
        "季起日值": 季聚合.first(),
        "季迄日值": 季聚合.last(),
        "季增減數": 季聚合.last() - 季聚合.first()
    })
    # 避免某些 pandas 版本在轉換 DatetimeIndex (Q) 到 PeriodIndex 時噴警告
    預估季均值.index = 預估季均值.index.to_period('Q')

    # 8. 封裝最終預測結果 (已加入最近歷史值日期與最後預估值日期)
    預測結果 = pd.Series({
        "預估值": 預估值,
        "預估季均值": 預估季均值,
        "rmse": 最佳_rmse,
        "mape": 最佳_mape,
        "模型名稱": 模型名稱,
        "歷史值數量": 歷史值數量,
        "預估值數量": 預測日數,
        "回測資料數": 回測天數,
        "模型參數": 最佳模型參數,
        "最近歷史值時間": 最近日期,
        "最後預估值時間": 最後預估日期
    })
    
    return 預測結果

def 預估至次年底每月值(
    歷月數值: "pd.Series",
    單位: str = '元'
) -> "pd.Series":
    """
    一、歷月數值之索引須為 pd.PeriodIndex(freq='M')，且不納入任何外部變數。
    二、利用 Optuna 最小化滾動盲測的 RMSE，動態尋找最佳「歷史記憶視窗大小」與「季節性模式」。
    三、底層採用經典 Theta Method，自動進行 12 個月季節性調整與趨勢外推。
    四、傳回預估每月值、預估每季總值、最近歷史值時間。
    """
    # 1. 於函式內部進行所有必要套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.forecasting.theta import ThetaModel
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 2. 數據型態檢查與型態防禦
    if not isinstance(歷月數值.index, pd.PeriodIndex) or not 歷月數值.index.freqstr.startswith('M'):
        raise ValueError(f"歷月數值索引必須為 pd.PeriodIndex(freq='M')，實際提供資料索引為 {type(歷月數值.index)}，頻率為 {getattr(歷月數值.index, 'freqstr', None)}")

    y_原始 = 歷月數值.astype(float)
    歷史月數 = len(y_原始)
    
    # 🛑 【新增防禦機制】如果資料過短，直接拋出易懂的錯誤，或是給予預設降級處理
    from twse_crawler.股利分析 import 無法預估盈餘
    最小所需月數 = 36  # 24個月訓練 + 12個月回測
    if 歷史月數 < 最小所需月數:
        raise 無法預估盈餘(f"輸入的歷史資料太短（僅 {歷史月數} 個月）。由於模型需要 12 個月滾動回測與至少 24 個月的歷史記憶視窗，資料至少需滿 {最小所需月數} 個月。")

    目標名稱 = y_原始.name if y_原始.name else '月營收'
    回測月數 = 12  # 採取 12 個月（完整一年）進行滾動盲測
    目標名稱 = y_原始.name if y_原始.name else '月營收'
    歷史月數 = len(y_原始)
    回測月數 = 12  # 採取 12 個月（完整一年）進行滾動盲測

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底，統一使用 freq='ME'）
    最近月份 = y_原始.index[-1]
    from zhongwen.時 import 今年數
    次年數 = 今年數 + 1

    未來下一月 = 最近月份 + 1
    次年底最後一月 = pd.Period(f"{次年數}-12", freq='M')
    未來月時軸 = pd.period_range(start=未來下一月, end=次年底最後一月, freq='M')
    外推步數 = len(未來月時軸)

    # =====================================================================
    # 🎯 4. 定義 Optuna 最佳化目標函數 (最小化滾動盲測下的預測誤差 RMSE)
    # =====================================================================
    def objective(trial):
        min_window = 24  
        max_window = 歷史月數 - 回測月數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)
            
        method_choice = trial.suggest_categorical("method", ["auto", "additive", "multiplicative"])

        單步回測值 = []
        真實對比值 = []

        for i in range(回測月數):
            當前終點 = 歷史月數 - 回測月數 + i
            訓練起點 = max(0, 當前終點 - window_size)

            y_訓練 = y_原始.iloc[訓練起點:當前終點]
            真實對比值.append(y_原始.iloc[當前終點])

            try:
                模型 = ThetaModel(y_訓練, period=12, method=method_choice)
                模型擬合 = 模型.fit()

                單月預測結果 = 模型擬合.forecast(1)
                單步回測值.append(單月預測結果.values[0])
            except:
                return float('inf')

        if len(單步回測值) < 回測月數:
            return float('inf')

        真實值_陣列 = np.array(真實對比值)
        回測值_陣列 = np.array(單步回測值)

        評估_mape = np.mean(np.abs((真實值_陣列 - 回測值_陣列) / 真實值_陣列))
        評估_rmse = np.sqrt(mean_squared_error(真實值_陣列, 回測值_陣列))

        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))

        return 評估_rmse

    # =====================================================================
    # 🚀 5. 啟動 Optuna 最佳化尋找最優超參數
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=30)

    最佳參數 = 研究工廠.best_params
    最佳視窗 = 最佳參數.get('window_size', 歷史月數 - 回測月數)
    最佳模式 = 最佳參數.get('method', 'auto')

    最佳實驗 = 研究工廠.best_trial
    mape_值 = 最佳實驗.user_attrs.get("mape", np.nan)
    rmse_值 = 最佳實驗.user_attrs.get("rmse", np.nan)

    # 6. 擬合最終 Theta 預測模型（限制在最佳記憶視窗內）
    最終訓練起點 = max(0, 歷史月數 - 最佳視窗)
    y_最終訓練 = y_原始.iloc[最終訓練起點:]

    最終模型 = ThetaModel(y_最終訓練, period=12, method=最佳模式)
    最終模型擬合 = 最終模型.fit()

    # 7. 外推未來預測值至次年底
    預估月_陣列 = 最終模型擬合.forecast(外推步數)
    預估月_序列 = pd.Series(預估月_陣列.values, index=未來月時軸)

    # =====================================================================
    # 📊 8. 整合與計算「預估每月值」與「預估每季總值」
    # =====================================================================
    預估每月全序列 = pd.concat([y_原始, 預估月_序列])

    預估每月值 = pd.DataFrame({
        f"預估每月{目標名稱}": 預估每月全序列
    }, index=預估每月全序列.index)

    預估每季總值 = 預估每月值.resample('Q').sum()
    預估每季總值.columns = [f"預估每季{目標名稱}"]

    # =====================================================================
    # 📈 9. 計算同比指標 (YoY)
    # =====================================================================
    if 歷史月數 > 12:
        最近歷史月值 = y_原始.iloc[-1]
        去年同月歷史值 = y_原始.iloc[-13]
        最近歷史月同比 = (最近歷史月值 / 去年同月歷史值) - 1 if 去年同月歷史值 != 0 else np.nan
    else:
        最近歷史月同比 = np.nan

    if 歷史月數 >= 12:
        預測首月值 = 預估月_序列.iloc[0]
        對應歷史同月值 = y_原始.iloc[-12]
        預測首月同比 = (預測首月值 / 對應歷史同月值) - 1 if 對應歷史同月值 != 0 else np.nan
    else:
        預測首月同比 = np.nan

    # 10. 傳回結果 Series (維持 pd.Period 型態)
    預測結果 = pd.Series({
        "預估每月值": 預估每月值,
        "預估每季總值": 預估每季總值,
        "歷史值數量": 歷史月數,
        "預估值數量": 外推步數,
        "回測資料數": 回測月數,
        "模型參數": 最佳參數,
        "最近歷史值時間": 最近月份,       # 這裡直接傳回 pd.Period 物件
        "最後預估值時間": 次年底最後一月,  # 這裡直接傳回 pd.Period 物件
        "最近歷史值同比": 最近歷史月同比,
        "首期預估值同比": 預測首月同比,
        "rmse": rmse_值,
        "mape": mape_值,
        "模型名稱": f"Theta"
    })
    return 預測結果

def 依外部季數據預估次年底數值(
    歷季值: "pd.Series",
    外部季數據表: "pd.DataFrame",
    預估目標 = '毛利率',
    單位 = '%'
) -> "pd.Series":
    """
    一、歷季值之索引與外部季數據表之索引皆須為 pd.PeriodIndex(freq='Q')。
    一、預估結果項目：預估各季值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                     模型名稱、模型參數、最近歷史值時間、最後預估值時間、
                     最近歷史值同比、首期預估值同比。
    二、預估季均值：季均值、季起日值、季迄日值、季增減數。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 2. 數據對齊、清洗與欄名自動推論
    if not isinstance(歷季值.index, pd.PeriodIndex):
        歷季值.index = pd.to_datetime(歷季值.index).to_period('Q')
    歷季值 = 歷季值.astype(float)
    
    if not isinstance(外部季數據表.index, pd.PeriodIndex):
        外部季數據表.index = pd.to_datetime(外部季數據表.index).to_period('Q')
    X_原始矩陣 = 外部季數據表.astype(float)
    
    # 動態推論所有外部特徵名稱
    外部數據名稱 = X_原始矩陣.columns.tolist()
    
    if len(外部數據名稱) == 0:
        raise ValueError("傳入的 外部季數據表 沒有任何特徵欄位！")
        
    回測季數 = 4

    # =====================================================================
    # 🎯 3. 定義 Optuna 最佳化目標函數 (以 AIC 為核心評測，自動適應多特徵欄位)
    # =====================================================================
    def objective(trial):
        # 參數 1：特徵的時間滯後效效
        lag = trial.suggest_int('lag_quarters', 0, 2)
        
        # 參數 2：歷史記憶視窗
        min_window = 12  
        max_window = len(歷季值) - 回測季數 - lag
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)
            
        # 參數 3：Newey-West 殘差修正滯後階數
        maxlags = trial.suggest_int('maxlags', 1, 3)

        # 依據選定的滯後階數進行特徵位移
        X_滯後矩陣 = X_原始矩陣.shift(lag)
        聯集資料 = pd.concat([歷季值, X_滯後矩陣], axis=1).dropna()
        
        if len(聯集資料) <= 回測季數 + 4:
            return float('inf')

        y_全 = 聯集資料[歷季值.name if 歷季值.name else 預估目標]
        X_全 = 聯集資料[外部數據名稱]
        X_全_含常數 = sm.add_constant(X_全)

        單步回測值 = []
        累加_aic = 0.0
        有效回測步數 = 0
        
        # 執行季度滾動盲測
        for i in range(回測季數):
            全資料當前終點 = len(聯集資料) - 回測季數 + i
            訓練起點 = max(0, 全資料當前終點 - window_size)
            
            X_訓練 = X_全_含常數.iloc[訓練起點:全資料當前終點]
            y_訓練 = y_全.iloc[訓練起點:全資料當前終點]
            X_測試 = X_全_含常數.iloc[[全資料當前終點]]
            
            try:
                模型 = sm.OLS(y_訓練, X_訓練).fit(cov_type='HAC', cov_kwds={'maxlags': maxlags})
                單季預測 = 模型.predict(X_測試)
                單步回測值.append(單季預測.values[0])
                
                累加_aic += 模型.aic
                有效回測步數 += 1
            except:
                return float('inf')
                
        if 有效回測步數 == 0:
            return float('inf')

        評估_aic = 累加_aic / 有效回測步數
        
        # 計算隨附的盲測驗證指標
        真實值 = y_全.iloc[-回測季數:]
        回測值_陣列 = np.array(單步回測值)
        
        評估_mape = np.mean(np.abs((真實值 - 回測值_陣列) / 真實值))
        評估_rmse = np.sqrt(mean_squared_error(真實值, 回測值_陣列))
        
        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))
        
        return 評估_aic

    # =====================================================================
    # 🚀 4. 啟動 Optuna 最小化 AIC
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=30)
    
    最佳參數 = 研究工廠.best_params
    最佳滯後 = 最佳參數['lag_quarters']
    最佳視窗 = 最佳參數.get('window_size', len(歷季值) - 回測季數 - 最佳滯後)
    最佳殘差階數 = 最佳參數['maxlags']

    # 5. 使用最優超參數重新建立歷史底稿
    X_最佳滯後矩陣 = X_原始矩陣.shift(最佳滯後)
    最終聯集資料 = pd.concat([歷季值, X_最佳滯後矩陣], axis=1).ffill().bfill()
    # from zhongwen.表 import 表示
    # 表示(最終聯集資料, 顯示索引=True)    
    y_歷史 = 最終聯集資料.loc[歷季值.index, 歷季值.name if 歷季值.name else 預估目標]
    X_歷史 = 最終聯集資料.loc[歷季值.index, 外部數據名稱]
    X_歷史_含常數 = sm.add_constant(X_歷史)

    # 6. 從最優實驗提取元數據
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # =====================================================================
    # 📈 7. 自動動態識別預測時間軸與計算同比（YoY）
    # =====================================================================
    最近季度 = 歷季值.index[-1]
    from zhongwen.時 import 今年數
    次年數 = 今年數 + 1
    
    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')

    # 計算：最近歷史值同比 (YoY)
    最近歷史值去年同季 = 最近季度 - 4
    if 最近歷史值去年同季 in y_歷史.index and y_歷史.loc[最近歷史值去年同季] != 0:
        # 適用於毛利率等百分比變動，若是成長率公式可用: (y_歷史.loc[最近季度] - y_歷史.loc[最近歷史值去年同季]) / y_歷史.loc[最近歷史值去年同季]
        # 這裡採用標準增減幅（若本身為百分比，通常看絕對變動或相對變動，此處依常規百分比成長率計算）
        最近歷史值同比 = (y_歷史.loc[最近季度] - y_歷史.loc[最近歷史值去年同季]) / abs(y_歷史.loc[最近歷史值去年同季])
    else:
        最近歷史值同比 = np.nan

    # 8. 擬合最終預測模型
    最終訓練起點 = max(0, len(y_歷史) - 最佳視窗)
    X_最終訓練 = X_歷史_含常數.iloc[最終訓練起點:]
    y_最終訓練 = y_歷史.iloc[最終訓練起點:]
    
    最終預測模型 = sm.OLS(y_最終訓練, X_最終訓練).fit(cov_type='HAC', cov_kwds={'maxlags': 最佳殘差階數})
    
    # 提取未來的外部特徵數據並預測
    X_未來 = 最終聯集資料.loc[未來季時軸, 外部數據名稱]
    X_未來_含常數 = sm.add_constant(X_未來, has_constant='add')
    預估價_陣列 = 最終預測模型.predict(X_未來_含常數)

    # 9. 整合並封裝「預估季價」數據框
    預估目標全序列 = pd.concat([y_歷史, 預估價_陣列])
    
    # 計算：首期預估值同比 (YoY)
    首期預估季度 = 未來季時軸[0]
    首期預估去年同季 = 首期預估季度 - 4
    if 首期預估去年同季 in 預估目標全序列.index and 預估目標全序列.loc[首期預估去年同季] != 0:
        首期預估值同比 = (預估目標全序列.loc[首期預估季度] - 預估目標全序列.loc[首期預估去年同季]) / abs(預估目標全序列.loc[首期預估去年同季])
    else:
        首期預估值同比 = np.nan

    # =====================================================================
    # 📦 10. 重新包裝傳回結果 Series
    # =====================================================================
    預測結果 = pd.Series({
        "預估各季值": 預估目標全序列,
        "rmse": rmse,
        "mape": mape,
        "歷史值數量": len(y_歷史),
        "預估值數量": len(預估價_陣列),
        "回測資料數": 回測季數,
        "模型名稱": "以 HAC 調整共變數之 OLS",
        "模型參數": 最佳參數,
        "最近歷史值時間": 最近季度,
        "最後預估值時間": 次年底最後一季,
        "最近歷史值同比": 最近歷史值同比,  # 新增項目
        "首期預估值同比": 首期預估值同比   # 新增項目
    })
    return 預測結果

def 預估至次年底每季值(
    歷季數值: "pd.Series",
    單位 = '元'
) -> "pd.Series":
    """
    一、預測結果：預估各季值。
    二、利用 Optuna 最小化滾動盲測的 AIC，動態尋找最佳「歷史記憶視窗大小」。
    三、底層採用 Holt-Winters 季度指數平滑模型，全方位加入空值防禦，適合業外損益等高波動科目。
    """
    # 1. 於函式內部進行套件導入 (維持高內聚設計)
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # =====================================================================
    # 🎯 2. 數據型態檢查、防禦與自動對齊 (徹底相容 Q-DEC 頻率陷阱)
    # =====================================================================
    # 檢查是否為 PeriodIndex 且屬於季度類型 (只要開頭是 'Q'，例如 Q, Q-DEC 皆相容)
    是季度索引 = isinstance(歷季數值.index, pd.PeriodIndex) and 歷季數值.index.freqstr.startswith('Q')
    
    if not 是季度索引:
        try:
            # 如果不是，嘗試強制轉換（相容 DatetimeIndex 或其他不標準的索引）
            歷季數值.index = pd.to_datetime(歷季數值.index).to_period('Q')
        except Exception as e:
            raise ValueError(
                f"歷季數值索引無法轉換為季度型態，實際提供資料索引為 {type(歷季數值.index)}、"
                f"{getattr(歷季數值.index, 'freqstr', '無頻率')}，錯誤原因: {e}"
            )
        
    # 🌟 強制將索引統一對齊為標準 'Q' 頻率，避免 statsmodels 拋出警告或錯誤
    y_原始 = 歷季數值.astype(float)
    y_原始.index = y_原始.index.asfreq('Q')
    
    回測季數 = 4  # 採取 4 季（完整一年）進行滾動盲測

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底）
    最近季度 = y_原始.index[-1]
    from zhongwen.時 import 今年數
    次年數 = 今年數 + 1
    
    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')
    外推步數 = len(未來季時軸)

    # =====================================================================
    # 🎯 4. 定義 Optuna 最佳化目標函數 (最小化滾動盲測下的 AIC)
    # =====================================================================
    def objective(trial):
        # 參數：歷史記憶視窗 (季度資料至少需要 12 季/3年，才能穩定識別 4 季的完整循環)
        min_window = 12  
        max_window = len(y_原始) - 回測季數
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)

        單步回測值 = []
        累加_aic = 0.0
        有效回測步數 = 0
        
        # 執行 4 季的歷史滾動盲測 (Walk-forward Validation)
        for i in range(回測季數):
            當前終點 = len(y_原始) - 回測季數 + i
            訓練起點 = max(0, 當前終點 - window_size)
            
            y_訓練 = y_原始.iloc[訓練起點:當前終點]
            
            try:
                # 建立 Holt-Winters 模型
                模型 = ExponentialSmoothing(
                    y_訓練, 
                    trend='add', 
                    seasonal='add', 
                    seasonal_periods=4,
                    initialization_method='estimated'
                )
                模型擬合 = 模型.fit()
                
                # 預測未來 1 步
                單季預測結果 = 模型擬合.forecast(1)
                預測值 = 單季預測結果.values[0]
                
                # 🌟 關鍵防禦一：如果模型返回的是 NaN 或 Inf，直接判定該歷史視窗失敗（給予無限大懲罰）
                if pd.isna(預測值) or np.isinf(預測值):
                    return float('inf')
                    
                單步回測值.append(預測值)
                
                # 累加模型在該視窗下的 AIC
                累加_aic += 模型擬合.aic
                有效回測步數 += 1
            except:
                return float('inf')
                
        # 🌟 關鍵防禦二：確保回測步數完整且未包含異常值
        if 有效回測步數 < 回測季數 or len(單步回測值) != 回測季數:
            return float('inf')

        回測值_陣列 = np.array(單步回測值)
        
        # 雙重防禦：確保陣列內容全為有限數值，避免 scikit-learn 報錯
        if not np.isfinite(回測值_陣列).all():
            return float('inf')

        評估_aic = 累加_aic / 有效回測步數
        
        # 計算隨附的驗證指標并存入 user_attrs
        真實值 = y_原始.iloc[-回測季數:]
        
        # 🌟 關鍵防禦三：分母加小量 (1e-5) 避免業外損益為 0 時導致除以零錯誤
        評估_mape = np.mean(np.abs((真實值 - 回測值_陣列) / (真實值 + 1e-5)))
        評估_rmse = np.sqrt(mean_squared_error(真實值, 回測值_陣列))
        
        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))
        
        return 評估_aic

    # =====================================================================
    # 🚀 5. 啟動 Optuna 最佳化尋找最優視窗
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=20)
    
    最佳參數 = 研究工廠.best_params
    最佳視窗 = 最佳參數.get('window_size', len(y_原始) - 回測季數)

    # 6. 提取最優實驗的驗證指標
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", np.nan)
    rmse = 最佳實驗.user_attrs.get("rmse", np.nan)

    # 7. 擬合最終 Holt-Winters 預估模型（限制在最佳記憶視窗內）
    最終訓練起點 = max(0, len(y_原始) - 最佳視窗)
    y_最終訓練 = y_原始.iloc[最終訓練起點:]
    
    最終模型 = ExponentialSmoothing(
        y_最終訓練, 
        trend='add', 
        seasonal='add', 
        seasonal_periods=4,
        initialization_method='estimated'
    )
    最終模型擬合 = 最終模型.fit()
    
    # 8. 外推未來預測值至次年底
    預估季_陣列 = 最終模型擬合.forecast(外推步數)
    
    # 🌟 關鍵防禦四：財務保守原則。若未來預測值發散噴出 NaN/Inf，以歷史最後 4 季平均值安全填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_最終訓練.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估季_序列 = pd.Series(預估季_陣列.values, index=未來季時軸)

    # =====================================================================
    # 📊 9. 整合與計算各項延伸統計指標
    # =====================================================================
    # 整合歷史與未來的全季度序列
    預估各季全序列 = pd.concat([y_原始, 預估季_序列])
    
    # 提取模型參數 (alpha, beta, gamma 等平滑係數)
    模型參數字典 = {k: v for k, v in 最終模型擬合.params.items() if not k.startswith('_') and v is not None}
    
    # 計算最近歷史值同比 (YoY)
    if len(y_原始) > 4:
        最近歷史值分母 = y_原始.iloc[-5]
        最近歷史值同比 = (y_原始.iloc[-1] - 最近歷史值分母) / 最近歷史值分母 if 最近歷史值分母 != 0 else np.nan
    else:
        最近歷史值同比 = np.nan
        
    # 計算首期預估值同比 (YoY)
    if len(y_原始) >= 3:  # 確保預測起點的前4季存在於原始資料中
        首期預估值分母 = y_原始.iloc[-4]
        首期預估值同比 = (預估季_序列.iloc[0] - 首期預估值分母) / 首期預估值分母 if 首期預估值分母 != 0 else np.nan
    else:
        首期預估值同比 = np.nan

    # 10. 傳回結果 Series
    預測結果 = pd.Series({
        "預估各季值": 預估各季全序列,
        "rmse": rmse,
        "mape": mape,
        "歷史值數量": len(y_原始),
        "預估值數量": 外推步數,
        "回測資料數": 回測季數,
        "模型名稱": "Holt-Winters Exponential Smoothing",
        "模型參數": 模型參數字典,
        "最近歷史值時間": 最近季度,          # 🌟 保持 pd.Period 格式
        "最後預估值時間": 次年底最後一季,    # 🌟 保持 pd.Period 格式
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })
    
    return 預測結果

def 以外部月數據預估次年底各月值(
    歷月值: "pd.Series",
    外部月數據表: "pd.DataFrame",
    預估目標 = '營利',  # 🌟 已將預設值改為 '營利'
    單位 = '元'        # 🌟 建議單位同步改為 '元' 或其他金額單位
) -> "pd.Series":
    """
    一、歷月值之索引與外部月數據表之索引皆須為 pd.PeriodIndex(freq='M')。
    二、預估結果項目：預估各月值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                     模型名稱、模型參數、最近歷史值時間、最後預估值時間、
                     最近歷史值同比、首期預估值同比。
    三、全程在純月頻率（Freq='M'）下進行數據對齊、滾動盲測與預測。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # 2. 數據對齊、清洗與欄名自動推論
    if not isinstance(歷月值.index, pd.PeriodIndex):
        歷月值.index = pd.to_datetime(歷月值.index).to_period('M')
        歷月值 = 歷月值.sort_index()
    歷月值 = 歷月值.astype(float)
    
    if not isinstance(外部月數據表.index, pd.PeriodIndex):
        外部月數據表.index = pd.to_datetime(外部月數據表.index).to_period('M')
        外部月數據表 = 外部月數據表.sort_index()
    X_原始矩陣 = 外部月數據表.astype(float)
    
    # 動態推論所有外部特徵名稱
    外部數據名稱 = X_原始矩陣.columns.tolist()
    
    if len(外部數據名稱) == 0:
        raise ValueError("外部月數據表無特徵欄位！")
        
    # 設定月度模型回測12個月即回測一個年度
    回測月數 = 12

    # =====================================================================
    # 🎯 3. 定義 Optuna 最佳化目標函數 (以 AIC 為核心，完全基於月頻率)
    # =====================================================================
    def objective(trial):
        # 參數 1：特徵的時間滯後效應（單位：月，搜尋 0 ~ 6 個月）
        lag = trial.suggest_int('lag_months', 0, 6)
        
        # 參數 2：歷史記憶視窗（月數據點較多，設定至少 24 個月）
        min_window = 24  
        max_window = len(歷月值) - 回測月數 - lag
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)
            
        # 參數 3：Newey-West 殘差修正滯後階數（月數據設定 3 ~ 6 個月較能修正自相關）
        maxlags = trial.suggest_int('maxlags', 3, 6)

        # 依據選定的滯後階數進行特徵位移
        X_滯後矩陣 = X_原始矩陣.shift(lag)
        聯集資料 = pd.concat([歷月值, X_滯後矩陣], axis=1).dropna()
        
        if len(聯集資料) <= 回測月數 + 12:
            return float('inf')

        y_全 = 聯集資料[歷月值.name if 歷月值.name else 預估目標]
        X_全 = 聯集資料[外部數據名稱]
        X_全_含常數 = sm.add_constant(X_全)

        單步回測值 = []
        累加_aic = 0.0
        有效回測步數 = 0
        
        # 執行月度滾動盲測
        for i in range(回測月數):
            全資料當前終點 = len(聯集資料) - 回測月數 + i
            訓練起點 = max(0, 全資料當前終點 - window_size)
            
            X_訓練 = X_全_含常數.iloc[訓練起點:全資料當前終點]
            y_訓練 = y_全.iloc[訓練起點:全資料當前終點]
            X_測試 = X_全_含常數.iloc[[全資料當前終點]]
            
            try:
                模型 = sm.OLS(y_訓練, X_訓練).fit(cov_type='HAC', cov_kwds={'maxlags': maxlags})
                單月預測 = 模型.predict(X_測試)
                單步回測值.append(單季預測.values[0] if '單季預測' in locals() else 單月預測.values[0])
                
                累加_aic += 模型.aic
                有效回測步數 += 1
            except:
                return float('inf')
                
        if 有效回測步數 == 0:
            return float('inf')

        評估_aic = 累加_aic / 有效回測步數
        
        # 計算隨附的盲測驗證指標
        真實值 = y_全.iloc[-回測月數:]
        回測值_陣列 = np.array(單步回測值)
        
        評估_mape = np.mean(np.abs((真實值 - 回測值_陣列) / 真實值))
        評估_rmse = np.sqrt(mean_squared_error(真實值, 回測值_陣列))
        
        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))
        
        return 評估_aic

    # =====================================================================
    # 🚀 4. 啟動 Optuna 最小化 AIC
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=30)
    
    最佳參數 = 研究工廠.best_params
    最佳滯後 = 最佳參數['lag_months']
    最佳視窗 = 最佳參數.get('window_size', len(歷月值) - 回測月數 - 最佳滯後)
    最佳殘差階數 = 最佳參數['maxlags']

    # 5. 使用最優超參數重新建立歷史底稿
    X_最佳滯後矩陣 = X_原始矩陣.shift(最佳滯後)
    最終聯集資料 = pd.concat([歷月值, X_最佳滯後矩陣], axis=1).ffill().bfill()
    
    y_歷史 = 最終聯集資料.loc[歷月值.index, 歷月值.name if 歷月值.name else 預估目標]
    X_歷史 = 最終聯集資料.loc[歷月值.index, 外部數據名稱]
    X_歷史_含常數 = sm.add_constant(X_歷史)

    # 6. 從最優實驗提取元數據
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # =====================================================================
    # 📈 7. 自動動態識別預測時間軸與計算同比（YoY）
    # =====================================================================
    最近月份 = 歷月值.index[-1]
    from zhongwen.時 import 今年數
    次年數 = 今年數 + 1
    
    未來下一月 = 最近月份 + 1
    次年底最後一月 = pd.Period(f"{次年數}-12", freq='M')
    未來月時軸 = pd.period_range(start=未來下一月, end=次年底最後一月, freq='M')

    # 計算：最近歷史值同比 (YoY) -> 分母加 abs() 以防營利為負數
    最近歷史月去年同月 = 最近月份 - 12
    if 最近歷史月去年同月 in y_歷史.index and y_歷史.loc[虧損或零值檢查 := 最近歷史月去年同月] != 0:
        最近歷史值同比 = (y_歷史.loc[最近月份] - y_歷史.loc[最近歷史月去年同月]) / abs(y_歷史.loc[最近歷史月去年同月])
    else:
        最近歷史值同比 = np.nan

    # 8. 擬合最終預測模型
    最終訓練起點 = max(0, len(y_歷史) - 最佳視窗)
    X_最終訓練 = X_歷史_含常數.iloc[最終訓練起點:]
    y_最終訓練 = y_歷史.iloc[最終訓練起點:]
    
    最終預測模型 = sm.OLS(y_最終訓練, X_最終訓練).fit(cov_type='HAC', cov_kwds={'maxlags': 最佳殘差階數})
    
    # 提取未來的外部月特徵數據並預測
    X_未來 = 最終聯集資料.loc[未來月時軸, 外部數據名稱]
    X_未來_含常數 = sm.add_constant(X_未來, has_constant='add')
    預估各月_陣列 = 最終預測模型.predict(X_未來_含常數)

    # 9. 整合並封裝「預估各月值」
    預估目標全序列 = pd.concat([y_歷史, 預估各月_陣列])
    
    # 計算：首期預估值同比 (YoY) -> 分母加 abs() 以防營利為負數
    首期預估月份 = 未來月時軸[0]
    首期預估去年同月 = 首期預估月份 - 12
    if 首期預估去年同月 in 預估目標全序列.index and 預估目標全序列.loc[首期預估去年同月] != 0:
        首期預估值同比 = (預估目標全序列.loc[首期預估月份] - 預估目標全序列.loc[首期預估去年同月]) / abs(預估目標全序列.loc[首期預估去年同月])
    else:
        首期預估值同比 = np.nan

    # =====================================================================
    # 📦 10. 重新包裝傳回結果 Series
    # =====================================================================
    預測結果 = pd.Series({
        "預估各月值": 預估目標全序列,
        "rmse": rmse,
        "mape": mape,
        "歷史值數量": len(y_歷史),
        "預估值數量": len(預估各月_陣列),
        "回測資料數": 回測月數,
        "模型名稱": "以 HAC 調整共變數之 OLS",
        "模型參數": 最佳參數,
        "最近歷史值時間": 最近月份,
        "最後預估值時間": 次年底最後一月,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })
    return 預測結果

def 以外部季數據預估次年底各月值(
    歷月值: "pd.Series",
    外部季數據表: "pd.DataFrame",
    預估目標 = '營利',  
    單位 = '元'        
) -> "pd.Series":
    """
    一、歷月值之索引須為 pd.PeriodIndex(freq='M')，外部季數據表之索引須為 pd.PeriodIndex(freq='Q')。
    二、預估結果項目：預估各月值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                     模型名稱、模型參數、最近歷史值時間、最後預估值時間、
                     最近歷史值同比、首期預估值同比。
    三、函式內部會自動將季數據（如合約負債）轉化並對齊至月頻率軸，全程在純月頻率（Freq='M'）下進行滾動盲測與預測。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # =====================================================================
    # 🛠️ 2. 數據對齊、季轉月頻率、清洗與時軸動態擴展
    # =====================================================================
    # 確保歷月值為月頻率 PeriodIndex
    if not isinstance(歷月值.index, pd.PeriodIndex):
        歷月值.index = pd.to_datetime(歷月值.index).to_period('M')
    歷月值 = 歷月值.sort_index().astype(float)
    
    # 確保外部數據表為季頻率 PeriodIndex
    if not isinstance(外部季數據表.index, pd.PeriodIndex):
        外部季數據表.index = pd.to_datetime(外部季數據表.index).to_period('Q')
    elif 外部季數據表.index.freqstr.startswith('M'):
        # 若誤傳為月頻率，轉換回季度
        外部季數據表.index = 外部季數據表.index.to_period('Q')
        
    外部季數據表 = 外部季數據表.sort_index()

    # 將季頻率索引轉為該季的「最後一個月」（例如：2025Q1 -> 2025-03）
    外部月映射表 = 外部季數據表.copy()
    外部月映射表.index = 外部月映射表.index.to_timestamp(how='E').to_period('M')

    # 自動推論時間軸：從歷史起點一路延伸到「次年底」
    最近月份 = 歷月值.index[-1]
    
    try:
        from zhongwen.時 import 今年數
        次年數 = 今年數 + 1
    except ImportError:
        次年數 = 最近月份.year + 1
        
    次年底最後一月 = pd.Period(f"{次年數}-12", freq='M')
    未來月時軸 = pd.period_range(start=最近月份 + 1, end=次年底最後一月, freq='M')
    全月時間軸 = pd.period_range(start=min(歷月值.index.min(), 外部月映射表.index.min()), end=次年底最後一月, freq='M')
    
    # 將轉換後的外部數據重新配置到全月時間軸，並向下填補（ffill），確保未來預測期也有特徵值可用
    X_原始矩陣 = 外部月映射表.reindex(全月時間軸).ffill().astype(float)
    
    # 動態推論所有外部特徵名稱
    外部數據名稱 = X_原始矩陣.columns.tolist()
    
    if len(外部數據名稱) == 0:
        raise ValueError("外部季數據表無特徵欄位！")
        
    # 設定月度模型回測12個月即回測一個年度
    回測月數 = 12

    # =====================================================================
    # 🎯 3. 定義 Optuna 最佳化目標函數 (以 AIC 為核心，完全基於月頻率)
    # =====================================================================
    def objective(trial):
        # 參數 1：特徵的時間滯後效應（單位：月，合約負債反映到營收通常在 0 ~ 6 個月）
        lag = trial.suggest_int('lag_months', 0, 6)
        
        # 參數 2：歷史記憶視窗
        min_window = 24  
        max_window = len(歷月值) - 回測月數 - lag
        if max_window <= min_window:
            window_size = min_window
        else:
            window_size = trial.suggest_int('window_size', min_window, max_window)
            
        # 參數 3：Newey-West 殘差修正滯後階數（修正營收季節性帶來的自相關）
        maxlags = trial.suggest_int('maxlags', 3, 6)

        # 依據選定的滯後階數進行特徵位移
        X_滯後矩陣 = X_原始矩陣.shift(lag)
        # 訓練時僅對齊有歷史營收的區間
        聯集資料 = pd.concat([歷月值, X_滯後矩陣], axis=1).dropna()
        
        if len(聯集資料) <= 回測月數 + 12:
            return float('inf')

        y_全 = 聯集資料[歷月值.name if 歷月值.name else 預估目標]
        X_全 = 聯集資料[外部數據名稱]
        X_全_含常數 = sm.add_constant(X_全)

        單步回測值 = []
        累加_aic = 0.0
        有效回測步數 = 0
        
        # 執行月度滾動盲測
        for i in range(回測月數):
            全資料當前終點 = len(聯集資料) - 回測月數 + i
            訓練起點 = max(0, 全資料當前終點 - window_size)
            
            X_訓練 = X_全_含常數.iloc[訓練起點:全資料當前終點]
            y_訓練 = y_全.iloc[訓練起點:全資料當前終點]
            X_測試 = X_全_含常數.iloc[[全資料當前終點]]
            
            try:
                # 使用 HAC (Newey-West) 修正異質變異與自相關
                模型 = sm.OLS(y_訓練, X_訓練).fit(cov_type='HAC', cov_kwds={'maxlags': maxlags})
                單月預測 = 模型.predict(X_測試)
                單步回測值.append(單月預測.values[0])
                
                累加_aic += 模型.aic
                有效回測步數 += 1
            except:
                return float('inf')
                
        if 有效回測步數 == 0:
            return float('inf')

        評估_aic = 累加_aic / 有效回測步數
        
        # 計算隨附的盲測驗證指標
        真實值 = y_全.iloc[-回測月數:]
        回測值_陣列 = np.array(單步回測值)
        
        評估_mape = np.mean(np.abs((真實值 - 回測值_陣列) / 真實值))
        評估_rmse = np.sqrt(mean_squared_error(真實值, 回測值_陣列))
        
        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))
        
        return 評估_aic

    # =====================================================================
    # 🚀 4. 啟動 Optuna 最小化 AIC
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=30)
    
    最佳參數 = 研究工廠.best_params
    最佳滯後 = 最佳參數['lag_months']
    最佳視窗 = 最佳參數.get('window_size', len(歷月值) - 回測月數 - 最佳滯後)
    最佳殘差階數 = 最佳參數['maxlags']

    # 5. 使用最優超參數重新建立歷史底稿
    X_最佳滯後矩陣 = X_原始矩陣.shift(最佳滯後)
    最終聯集資料 = pd.concat([歷月值, X_最佳滯後矩陣], axis=1).ffill().bfill()
    
    y_歷史 = 最終聯集資料.loc[歷月值.index, 歷月值.name if 歷月值.name else 預估目標]
    X_歷史 = 最終聯集資料.loc[歷月值.index, 外部數據名稱]
    X_歷史_含常數 = sm.add_constant(X_歷史)

    # 6. 從最優實驗提取元數據
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # =====================================================================
    # 📈 7. 自動動態識別預測時間軸與計算同比（YoY）
    # =====================================================================
    # 計算：最近歷史值同比 (YoY)
    最近歷史月去年同月 = 最近月份 - 12
    if 最近歷史月去年同月 in y_歷史.index and y_歷史.loc[虧損或零值檢查 := 最近歷史月去年同月] != 0:
        最近歷史值同比 = (y_歷史.loc[最近月份] - y_歷史.loc[最近歷史月去年同月]) / abs(y_歷史.loc[最近歷史月去年同月])
    else:
        最近歷史值同比 = np.nan

    # 8. 擬合最終預測模型
    最終訓練起點 = max(0, len(y_歷史) - 最佳視窗)
    X_最終訓練 = X_歷史_含常數.iloc[最終訓練起點:]
    y_最終訓練 = y_歷史.iloc[最終訓練起點:]
    
    最終預測模型 = sm.OLS(y_最終訓練, X_最終訓練).fit(cov_type='HAC', cov_kwds={'maxlags': 最佳殘差階數})
    
    # 提取未來的外部月特徵數據並預測
    X_未來 = 最終聯集資料.loc[未來月時軸, 外部數據名稱]
    X_未來_含常數 = sm.add_constant(X_未來, has_constant='add')
    預估各月_陣列 = 最終預測模型.predict(X_未來_含常數)
    預估各月_序列 = pd.Series(預估各月_陣列, index=未來月時軸)

    # 9. 整合並封裝「預估各月值」
    預估目標全序列 = pd.concat([y_歷史, 預估各月_序列])
    
    # 計算：首期預估值同比 (YoY)
    首期預估月份 = 未來月時軸[0]
    首期預估去年同月 = 首期預估月份 - 12
    if 首期預估去年同月 in 預估目標全序列.index and 預估目標全序列.loc[首期預估去年同月] != 0:
        首期預估值同比 = (預估目標全序列.loc[首期預估月份] - 預估目標全序列.loc[首期預估去年同月]) / abs(預估目標全序列.loc[首期預估去年同月])
    else:
        首期預估值同比 = np.nan

    # =====================================================================
    # 📦 10. 重新包裝傳回結果 Series
    # =====================================================================
    預測結果 = pd.Series({
        "預估各月值": 預估目標全序列,
        "rmse": rmse,
        "mape": mape,
        "歷史值數量": len(y_歷史),
        "預估值數量": len(預估各月_陣列),
        "回測資料數": 回測月數,
        "模型名稱": "以 HAC 調整共變數之 OLS",
        "模型參數": 最佳參數,
        "最近歷史值時間": 最近月份,
        "最後預估值時間": 次年底最後一月,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })
    return 預測結果

def 以外部季數據預估次年底各月值乙式(
    歷月值: "pd.Series",
    外部季數據表: "pd.DataFrame",
    預估目標 = '營利',  
    單位 = '元'        
) -> "pd.Series":
    """
    【雙階段殘差組合模型 (Theta Baseline + OLS Residual Adjustment)】
    一、歷月值之索引須為 pd.PeriodIndex(freq='M')，外部季數據表之索引須為 pd.PeriodIndex(freq='Q')。
    二、預估結果項目：預估各月值（實際歷史值 + 未來組合預測值）、rmse、mape、歷史值數量、
                     預估值數量、回測資料數、模型名稱、模型參數、最近歷史值時間、
                     最後預估值時間、最近歷史值同比、首期預估值同比。
    三、核心邏輯：
        階段 1：由 Theta 模型捕捉月營收極強的季節性基本盤。
        階段 2：利用逐步單步 forecast(1) 的方式安全建立 Theta 歷史擬合值，求出殘差，
               並由 Optuna 自動尋找「合約負債」最佳滯後月數（Lag）與視窗，以 OLS 修正該殘差。
    """
    # 1. 於函式內部進行套件導入
    import warnings
    import numpy as np
    import pandas as pd
    import optuna
    import statsmodels.api as sm
    from statsmodels.tsa.forecasting.theta import ThetaModel
    from sklearn.metrics import mean_squared_error

    warnings.filterwarnings("ignore")
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    # =====================================================================
    # 🛠️ 2. 數據對齊、季轉月頻率、清洗與時軸動態擴展
    # =====================================================================
    if not isinstance(歷月值.index, pd.PeriodIndex):
        歷月值.index = pd.to_datetime(歷月值.index).to_period('M')
    歷月值 = 歷月值.sort_index().astype(float)
    
    if not isinstance(外部季數據表.index, pd.PeriodIndex):
        外部季數據表.index = pd.to_datetime(外部季數據表.index).to_period('Q')
    elif 外部季數據表.index.freqstr.startswith('M'):
        外部季數據表.index = 外部季數據表.index.to_period('Q')
        
    外部季數據表 = 外部季數據表.sort_index()

    # 將季頻率轉為該季的最後一個月（2025Q1 -> 2025-03）
    外部月映射表 = 外部季數據表.copy()
    外部月映射表.index = 外部月映射表.index.to_timestamp(how='E').to_period('M')

    最近月份 = 歷月值.index[-1]
    
    try:
        from zhongwen.時 import 今年數
        次年數 = 今年數 + 1
    except ImportError:
        次年數 = 最近月份.year + 1
        
    次年底最後一月 = pd.Period(f"{次年數}-12", freq='M')
    未來月時軸 = pd.period_range(start=最近月份 + 1, end=次年底最後一月, freq='M')
    全月時間軸 = pd.period_range(start=min(歷月值.index.min(), 外部月映射表.index.min()), end=次年底最後一月, freq='M')
    
    # 擴展外部特徵至全月時間軸並向下填補
    X_原始矩陣 = 外部月映射表.reindex(全月時間軸).ffill().astype(float)
    外部數據名稱 = X_原始矩陣.columns.tolist()
    
    if len(外部數據名稱) == 0:
        raise ValueError("外部季數據表無特徵欄位！")
        
    回測月數 = 12

    # =====================================================================
    # 🎯 3. 定義 Optuna 最佳化目標函數 (評估雙階段組合後的綜合表現)
    # =====================================================================
    def objective(trial):
        lag = trial.suggest_int('lag_months', 0, 6)
        
        min_window = 24  
        max_window = len(歷月值) - 回測月數 - lag
        window_size = min_window if max_window <= min_window else trial.suggest_int('window_size', min_window, max_window)
        maxlags = trial.suggest_int('maxlags', 3, 6)

        X_滯後矩陣 = X_原始矩陣.shift(lag)
        X_全_時軸_含常數 = sm.add_constant(X_滯後矩陣, has_constant='add')

        單步組合回測值 = []
        累加_aic_ols = 0.0
        有效回測步數 = 0
        
        # 執行雙階段月度滾動盲測
        for i in range(回測月數):
            全資料當前終點 = len(歷月值) - 回測月數 + i
            測試月份 = 歷月值.index[全資料當前終點]
            
            # -----------------------------------------------------------------
            # 階段 1：當前盲測視窗的 Theta 預測與歷史殘差計算
            # -----------------------------------------------------------------
            y_歷史_當前 = 歷月值.iloc[:全資料當前終點]
            
            y_歷史_當前_ts = y_歷史_當前.copy()
            y_歷史_當前_ts.index = y_歷史_當前_ts.index.to_timestamp()
            
            try:
                # 建立當前時間點的基礎模型與下一單月預測
                theta_模型 = ThetaModel(y_歷史_當前_ts, period=12).fit()
                theta_單月預測 = theta_模型.forecast(1).values[0]
                
                # 🛠️ 修正處 1：為了取得 y_歷史_當前 對應的歷史擬合值，
                # 我們往前滾動計算最近 24 期的擬合表現來提供給 OLS 作為殘差序列
                擬合長度 = min(24, len(y_歷史_當前) - 13)
                if 擬合長度 < 12:
                    return float('inf')
                
                歷史擬合列表 = []
                歷史擬合索引 = y_歷史_當前.index[-擬合長度:]
                
                for idx_m in 歷史擬合索引:
                    切片終點 = y_歷史_當前.index.get_loc(idx_m)
                    y_切片_ts = y_歷史_當前.iloc[:切片終點].copy()
                    y_切片_ts.index = y_切片_ts.index.to_timestamp()
                    t_mod = ThetaModel(y_切片_ts, period=12).fit()
                    歷史擬合列表.append(t_mod.forecast(1).values[0])
                
                theta_歷史擬合 = pd.Series(歷史擬合列表, index=歷史擬合索引)
                y_歷史_殘差 = (y_歷史_當前.loc[歷史擬合索引] - theta_歷史擬合).dropna()
                
                # -----------------------------------------------------------------
                # 階段 2：用 OLS 模型擬合殘差
                # -----------------------------------------------------------------
                可用時軸 = y_歷史_殘差.index
                X_訓練_ols = X_全_時軸_含常數.loc[可用時軸].iloc[-window_size:]
                y_訓練_ols = y_歷史_殘差.loc[X_訓練_ols.index]
                
                模型_ols = sm.OLS(y_訓練_ols, X_訓練_ols).fit(cov_type='HAC', cov_kwds={'maxlags': maxlags})
                
                X_測試_ols = X_全_時軸_含常數.loc[[測試月份]]
                ols_單月預測 = 模型_ols.predict(X_測試_ols).values[0]
                
                組合單月預測 = theta_單月預測 + ols_單月預測
                單步組合回測值.append(組合單月預測)
                
                累加_aic_ols += 模型_ols.aic
                有效回測步數 += 1
            except:
                return float('inf')
                
        if 有效回測步數 < 回測月數:
            return float('inf')

        真實值 = 歷月值.iloc[-回測月數:]
        回測值_陣列 = np.array(單步組合回測值)
        
        評估_mape = np.mean(np.abs((真實值 - 回測值_陣列) / 真實值))
        評估_rmse = np.sqrt(mean_squared_error(真實值, 回測值_陣列))
        
        trial.set_user_attr("mape", float(評估_mape))
        trial.set_user_attr("rmse", float(評估_rmse))
        
        return 累加_aic_ols / 有效回測步數

    # =====================================================================
    # 🚀 4. 啟動 Optuna 最小化組合盲測殘差的 OLS AIC
    # =====================================================================
    研究工廠 = optuna.create_study(direction='minimize')
    研究工廠.optimize(objective, n_trials=30)
    
    最佳參數 = 研究工廠.best_params
    最佳滯後 = 最佳參數['lag_months']
    最佳視窗 = 最佳參數.get('window_size', len(歷月值) - 回測月數 - 最佳滯後)
    最佳殘差階數 = 最佳參數['maxlags']

    # =====================================================================
    # 📈 5. 使用全歷史數據重新擬合最終的雙階段組合模型
    # =====================================================================
    # 5-1. 最終階段 1：完整歷史的 Theta 擬合與未來預測
    歷月值_ts = 歷月值.copy()
    歷月值_ts.index = 歷月值_ts.index.to_timestamp()
    最終_theta_模型 = ThetaModel(歷月值_ts, period=12).fit()
    
    未來_theta_預測_陣列 = 最終_theta_模型.forecast(len(未來月時軸)).values
    未來_theta_預測_序列 = pd.Series(未來_theta_預測_陣列, index=未來月時軸)
    
    # 🛠️ 修正處 2：最終模型同樣採用全公開、無損的 forecast 循環建構最終的歷史殘差
    最終擬合長度 = min(36, len(歷月值) - 13)
    最終擬合列表 = []
    最終擬合索引 = 歷月值.index[-最終擬合長度:]
    
    for idx_m in 最終擬合索引:
        切片終點 = 歷月值.index.get_loc(idx_m)
        y_切片_ts = 歷月值.iloc[:切片終點].copy()
        y_切片_ts.index = y_切片_ts.index.to_timestamp()
        t_mod = ThetaModel(y_切片_ts, period=12).fit()
        最終擬合列表.append(t_mod.forecast(1).values[0])
        
    最終_theta_歷史擬合 = pd.Series(最終擬合列表, index=最終擬合索引)
    最終_歷史殘差 = (歷月值.loc[最終擬合索引] - 最終_theta_歷史擬合).dropna()

    # 5-2. 最終階段 2：完整歷史殘差的 OLS 擬合與未來預測
    X_最佳滯後矩陣 = X_原始矩陣.shift(最佳滯後)
    X_最終_時軸_含常數 = sm.add_constant(X_最佳滯後矩陣, has_constant='add')
    
    X_最終訓練_ols = X_最終_時軸_含常數.loc[最終_歷史殘差.index].iloc[-最佳視窗:]
    y_最終訓練_ols = 最終_歷史殘差.loc[X_最終訓練_ols.index]
    
    最終_ols_模型 = sm.OLS(y_最終訓練_ols, X_最終訓練_ols).fit(cov_type='HAC', cov_kwds={'maxlags': 最佳殘差階數})
    
    X_未來_ols = X_最終_時軸_含常數.loc[未來月時軸]
    未來_ols_預測_陣列 = 最終_ols_模型.predict(X_未來_ols).values
    未來_ols_預測_序列 = pd.Series(未來_ols_預測_陣列, index=未來月時軸)

    # 5-3. 雙階段未來預測值最終融合
    未來_組合預測_序列 = 未來_theta_預測_序列 + 未來_ols_預測_序列

    # 6. 從最優實驗提取綜合盲測元數據
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # 7. 整合歷史與未來預測序列
    預估目標全序列 = pd.concat([歷月值, 未來_組合預測_序列])

    # =====================================================================
    # 📊 8. 計算同比 (YoY)
    # =====================================================================
    最近歷史值同比 = np.nan
    最近歷史月去年同月 = 最近月份 - 12
    if 最近歷史月去年同月 in 歷月值.index and 歷月值.loc[最近歷史月去年同月] != 0:
        recently_val = 歷月值.loc[最近月份]
        base_val = 歷月值.loc[最近歷史月去年同月]
        最近歷史值同比 = (recently_val - base_val) / abs(base_val)

    首期預估值同比 = np.nan
    首期預估月份 = 未來月時軸[0]
    首期預估去年同月 = 首期預估月份 - 12
    if 首期預估去年同月 in 預估目標全序列.index and 預估目標全序列.loc[首期預估去年同月] != 0:
        f_val = 預估目標全序列.loc[首期預估月份]
        f_base_val = 預估目標全序列.loc[首期預估去年同月]
        首期預估值同比 = (f_val - f_base_val) / abs(f_base_val)

    # =====================================================================
    # 📦 9. 包裝傳回結果 Series
    # =====================================================================
    預測結果 = pd.Series({
        "預估各月值": 預估目標全序列,
        "rmse": rmse,
        "mape": mape,
        "歷史值數量": len(歷月值),
        "預估值數量": len(未來_組合預測_序列),
        "回測資料數": 回測月數,
        "模型名稱": "Theta Baseline + OLS Residual HAC 組合模型",
        "模型參數": 最佳參數,
        "最近歷史值時間": 最近月份,
        "最後預估值時間": 次年底最後一月,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比
    })
    return 預測結果

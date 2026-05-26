
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

def 表達預估方法(預估結果, 預估目標='業外損益', 單位='元', 時間單位='季'):
    from zhongwen.數 import 取最簡約數
    m = 預估結果
    if 時間單位=='季':
        訓練資料說明 = f'依{表達期間(m.最近歷史值季度)}前{m.歷史值數量:,}{時間單位}{預估目標}訓練'
        預估數量說明 = f'預估至{m.最後預估值季度.year-1911:,}年底之{m.預估值數量:,}{時間單位}{預估目標}'
    elif 時間單位=='月':
        訓練資料說明 = f'依{表達期間(m.最近歷史值月份)}前{m.歷史月數:,}月{時間單位}{預估目標}訓練'
        預估數量說明 = f'預估至{m.最後預估值月份.year-1911:,}年底之{m.預測月數:,}{時間單位}{預估目標}'
    else:
        訓練資料說明 = f'依{表達期間(m.最近歷史值日期)}前{m.歷史值數量:,}{時間單位}{預估目標}訓練'
        預估數量說明 = f'預估至{m.最後預估值日期.year-1911:,}年底之{m.預估值數量:,}{時間單位}{預估目標}'
    return (f'{訓練資料說明}'
    f'得回測{m.回測資料數:,}{時間單位}之變動範圍為{取最簡約數(m.rmse)}{單位}，比率{m.mape:,.2%}'
    f'{m.模型名稱}模型，{預估數量說明}'
    )

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

def 表達預測準確度(預測結果, 單位='%') -> str:
    return 表達模型精準度(預測結果.rmse, 預測結果.mape, 單位)

def 預估至次年底每季值(
    歷季數值: "pd.Series",
    單位 = '元'
) -> "pd.Series":
    """
    一、傳回預估每季值及模型準確度說明。
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
    
    目標名稱 = y_原始.name if y_原始.name else '季營收'
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
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # 7. 封裝模型準確度說明 (對接外部的表達模型精準度函式)
    模型準確度說明 = 表達模型精準度(rmse, mape, 單位)
    模型準確度說明 += f"，滾動{最佳視窗:,.0f}季視窗"

    # 8. 擬合最終 Holt-Winters 預估模型（限制在最佳記憶視窗內）
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
    
    # 9. 外推未來預測值至次年底
    預估季_陣列 = 最終模型擬合.forecast(外推步數)
    
    # 🌟 關鍵防禦四：財務保守原則。若未來預測值發散噴出 NaN/Inf，以歷史最後 4 季平均值安全填充
    if 預估季_陣列.isna().any() or np.isinf(預估季_陣列.values).any():
        安全填補值 = y_最終訓練.tail(4).mean()
        安全填補值 = 安全填補值 if pd.notna(安全填補值) else 0.0
        預估季_陣列 = 預估季_陣列.fillna(安全填補值).replace([np.inf, -np.inf], 安全填補值)
        
    預估季_序列 = pd.Series(預估季_陣列.values, index=未來季時軸)

    # =====================================================================
    # 📊 10. 整合「預估每季總值」
    # =====================================================================
    # 整合歷史與未來的全季度序列
    預估每季全序列 = pd.concat([y_原始, 預估季_序列])
    
    # 11. 傳回結果 Series
    預測結果 = pd.Series({
        "預估每季值": 預估每季全序列
       ,"模型準確度說明": 模型準確度說明
    })
    
    return 預測結果

def 預估次年底值(歷日值: "pd.Series", 預估目標 = '鉛價', 單位 = '美元'):
    """
    一、歷日值索引須是 DatetimeIndex。
    三、預估結果項目：預估值、預估季均值、rmse、mape、歷史值數量、預估值數量、回測資料數、
                      模型名稱、模型參數、最近歷史值日期、最後預估值日期。
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
        模型名稱 = "ThetaModel(GridSearch_Failed_Fallback_To_Auto)"
    else:
        模型名稱 = "ThetaModel(GridSearch_Optimized)"

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
        "最近歷史值日期": 最近日期,
        "最後預估值日期": 最後預估日期
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
    二、傳回包含預估各季值、各式模型評估與元數據的 pd.Series。
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
        # 參數 1：特徵的時間滯後效應
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
    
    y_歷史 = 最終聯集資料.loc[歷季值.index, 歷季值.name if 歷季值.name else 預估目標]
    X_歷史 = 最終聯集資料.loc[歷季值.index, 外部數據名稱]
    X_歷史_含常數 = sm.add_constant(X_歷史)

    # 6. 從最優實驗提取元數據
    最佳實驗 = 研究工廠.best_trial
    mape = 最佳實驗.user_attrs.get("mape", 0.0)
    rmse = 最佳實驗.user_attrs.get("rmse", 0.0)

    # 7. 自動動態識別預測時間軸
    最近季度 = 歷季值.index[-1]
    from zhongwen.時 import 今年數
    次年數 = 今年數 + 1
    
    未來下一季 = 最近季度 + 1
    次年底最後一季 = pd.Period(f"{次年數}Q4", freq='Q')
    未來季時軸 = pd.period_range(start=未來下一季, end=次年底最後一季, freq='Q')

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
        "模型名稱": "Ordinary Least Squares (OLS) with HAC Covariance",
        "模型參數": 最佳參數,
        "最近歷史值季度": 最近季度,
        "最後預估值季度": 次年底最後一季
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
    四、傳回預估每月值、預估每季總值，以及各項模型評估指標與時間範圍。
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
    目標名稱 = y_原始.name if y_原始.name else '月營收'
    歷史月數 = len(y_原始)
    回測月數 = 12  # 採取 12 個月（完整一年）進行滾動盲測

    # 3. 動態識別並建構預測時間軸（向未來外推至次年底，統一使用 freq='ME'）
    最近月份 = y_原始.index[-1]
    今年數 = 最近月份.year
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
        "歷史月數": 歷史月數,
        "預測月數": 外推步數,
        "回測資料數": 回測月數,
        "模型參數": 最佳參數,
        "最近歷史值月份": 最近月份,       # 這裡直接傳回 pd.Period 物件
        "最後預估值月份": 次年底最後一月,  # 這裡直接傳回 pd.Period 物件
        "最近歷史月同比": 最近歷史月同比,
        "預測首月同比": 預測首月同比,
        "rmse": rmse_值,
        "mape": mape_值,
        "模型名稱": f"ThetaModel (Window: {最佳視窗}, Method: {最佳模式})"
    })
    return 預測結果

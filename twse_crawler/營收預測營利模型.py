import numpy as np
import optuna
import pandas as pd
import statsmodels.api as sm
from twse_crawler.預估次年底 import 表達期間
from zhongwen.程式 import 通知執行時間
from twse_crawler.蒐整財務資訊 import 增加股票分析函數依資料時間更新快取功能
from diskcache import Cache, Index
from pathlib import Path
import logging

營收預測營利結果快取檔 = Index(str(Path.home() / '.twse_crawler' / '快取' / '營收預測營利結果快取檔'))
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
logger = logging.getLogger(Path(__file__).stem)

def 表達預估說明乙(預估結果, 預估目標='毛利率', 時間單位='季'):
    from zhongwen.數 import 取增減百分比
    from twse_crawler.預估次年底 import 表達期間
    return f'{表達期間(預估結果.最近歷史值時間)}{預估目標}同比{取增減百分比(預估結果.最近歷史值同比)}，預估次{時間單位}同比{取增減百分比(預估結果.首期預估值同比)}，誤差{預估結果.wape:,.0%}'

def 表達預估方法乙(預估結果, 預估目標='業外損益', 單位='元', 時間單位='季'):
    from zhongwen.數 import 取最簡約數
    m = 預估結果
    訓練資料說明 = f'依{表達期間(m.最近歷史值時間)}前{m.歷史值數量:,}{時間單位}{預估目標}回測'
    預估數量說明 = f'預估至{m.最後預估值時間.year-1911:,}年底之{m.預估值數量:,}{時間單位}{預估目標}'
    預估誤差範圍說明 = f'所得以過去{m.回測資料數:,}{時間單位}訓練，誤差{m.wape:,.2%}'
    預估誤差範圍說明 += f'，較無腦預測改善{m['較無腦預測改善率']:,.2%}'
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
        print(m.模型參數)
        模型參數 = (f'，以近{m.模型參數["window_size"]}{時間單位}{預估目標}訓練')
    elif m.模型名稱 == 'Theta乙式':
        模型參數 = (f'，以近{m.模型參數["window_size"]}{時間單位}{預估目標}訓練')
    if m.模型名稱 == 'OLS':
        模型參數 = ''
    return (
        f'{訓練資料說明}{預估誤差範圍說明}'
        f'{模型參數}'
        f'之{m.模型名稱}模型，{預估數量說明}'
    )

def calculate_wape(y_true, y_pred, y_train, seasonal_period=1):
    """計算 OLS 模型與 Naive 模型的 WAPE，並比較 OLS 改善了多少

    分母皆採用 sum(|y_true|)
    - seasonal_period = 1  : 一般 WAPE (Naive 為上一期 y_{t-1})
    - seasonal_period = 4  : 季節性 sWAPE (Naive 為去年同期 y_{t-4})
    """
    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    y_train = np.array(y_train)

    sum_abs_y = np.sum(np.abs(y_true))
    if sum_abs_y == 0:
        return np.nan, np.nan, np.nan

    # 1. 計算 OLS 模型之 WAPE
    wape_ols = np.sum(np.abs(y_true - y_pred)) / sum_abs_y

    # 2. 計算 Naive 模型預測值與 WAPE
    # 這裡從歷史訓練集 y_train 取出對應的前 1 期或前 4 期真實值
    m = seasonal_period
    # 針對步階驗證 (Walk-forward)，y_train 的最後 m 個元素即為 Naive 預測基準
    y_naive = y_train[-m:] if len(y_train) >= m else y_train[-1:]

    # 若測試集點數與 naive 長度不一（一般逐步驗證為 1 點），取最後一個對應值
    y_naive_pred = y_naive[-len(y_true) :]
    wape_naive = np.sum(np.abs(y_true - y_naive_pred)) / sum_abs_y

    # 3. 計算 OLS 較 Naive 改善多少 (提升百分點/幅度)
    wape_improvement = wape_naive - wape_ols

    return wape_ols, wape_naive, wape_improvement


def detect_data_pattern(y_series, seasonal_period=4):
    """備援機制：自動偵測歷史數據模式，回傳 'seasonal' (季節性) 或 'growth' (成長型/單調趨勢型)"""
    y = np.array(y_series)
    n = len(y)

    if n < seasonal_period * 2:
        return "growth"

    x = np.arange(n)
    x_const = sm.add_constant(x)
    trend_model = sm.OLS(y, x_const).fit()
    p_value_trend = trend_model.pvalues[1]
    r_squared = trend_model.rsquared

    diff_1 = np.diff(y, n=1)
    diff_4 = y[seasonal_period:] - y[:-seasonal_period]

    var_diff_1 = np.var(diff_1) if len(diff_1) > 0 else 1e-5
    var_diff_4 = np.var(diff_4) if len(diff_4) > 0 else 1e-5

    seasonal_ratio = var_diff_4 / var_diff_1 if var_diff_1 > 0 else 1.0

    if r_squared > 0.65 and p_value_trend < 0.05:
        return "growth"
    elif seasonal_ratio < 0.85:
        return "seasonal"
    else:
        return "growth"

# =====================================================================
# 1. 模型訓練與優化函數 (改用 WAPE / sWAPE 評估)
# =====================================================================
def 取季營收預測營利模型(
    歷季財報,
    最小訓練季數=8,
    最大訓練季數=28,
    優化試驗次數=30,
    指定誤差衡量指標=None,
):
    """
    一、分析歷史損益資料，同時評估 WAPE 與 sWAPE，優先採用誤差最小者；
    二、計算 OLS 模型的 WAPE 及其相較於 Naive 模型的改善程度。
    三、參數項目：模型、固定成本、變動成本率、變動營利率、訓練季數、評估指標、
                  OLS_WAPE、Naive_WAPE、WAPE較Naive改善量、選擇原因、最近季度
    """
    optuna.logging.set_verbosity(optuna.logging.WARNING)

    df_pnl = 歷季財報.copy().sort_values("財報季度")

    if "營業成本" not in df_pnl.columns:
        df_pnl["營業成本"] = df_pnl["營收"] - df_pnl["營利"]

    rev_hist = df_pnl["營收"].values
    cost_hist = df_pnl["營業成本"].values
    n_samples = len(df_pnl)

    actual_min_window = max(最小訓練季數, 7)
    max_possible_window = max(actual_min_window + 1, n_samples - 4)
    actual_max_window = min(最大訓練季數, max_possible_window)

    user_metric = (
        指定誤差衡量指標.lower()
        if isinstance(指定誤差衡量指標, str)
        else None
    )

    if user_metric in ["wape", "swape"]:
        target_metrics = [user_metric]
    else:
        target_metrics = ["wape", "swape"]

    metric_results = {}

    for metric in target_metrics:
        period = 4 if metric == "swape" else 1

        def objective(trial):
            window_size = trial.suggest_int(
                "window_size", actual_min_window, actual_max_window
            )

            y_true_list = []
            y_pred_list = []
            y_naive_list = []

            for i in range(window_size, n_samples):
                X_train = rev_hist[i - window_size : i]
                y_train = cost_hist[i - window_size : i]

                X_test = rev_hist[i]
                y_test = cost_hist[i]

                # OLS 擬合
                X_train_const = sm.add_constant(X_train)
                ols = sm.OLS(y_train, X_train_const).fit()
                pred_cost = ols.params[0] + ols.params[1] * X_test

                # Naive 基準預測值 (前 1 期或前 4 期)
                naive_cost = (
                    y_train[-period]
                    if len(y_train) >= period
                    else y_train[-1]
                )

                y_true_list.append(y_test)
                y_pred_list.append(pred_cost)
                y_naive_list.append(naive_cost)

            # 計算整個滾動驗證區間的整體 WAPE（分子與分母先加總再相除）
            sum_abs_y = np.sum(np.abs(y_true_list))
            if sum_abs_y == 0:
                return float("inf")

            wape_ols = np.sum(np.abs(np.array(y_true_list) - np.array(y_pred_list))) / sum_abs_y
            wape_naive = np.sum(np.abs(np.array(y_true_list) - np.array(y_naive_list))) / sum_abs_y
            wape_imp = wape_naive - wape_ols

            # 暫存本輪結果細節
            trial.set_user_attr("wape_ols", wape_ols)
            trial.set_user_attr("wape_naive", wape_naive)
            trial.set_user_attr("wape_imp", wape_imp)

            return wape_ols

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=優化試驗次數)

        best_trial = study.best_trial
        metric_results[metric] = {
            "best_window": best_trial.params["window_size"],
            "best_score": best_trial.value,  # 此即為 wape_ols
            "wape_naive": best_trial.user_attrs.get("wape_naive", np.nan),
            "wape_imp": best_trial.user_attrs.get("wape_imp", np.nan),
        }

    # 指標判定邏輯
    if user_metric in ["wape", "swape"]:
        selected_metric = user_metric.upper()
        res = metric_results[user_metric]
        selection_reason = "使用者指定指標"
    else:
        wape_score = metric_results["wape"]["best_score"]
        swape_score = metric_results["swape"]["best_score"]

        if (
            not np.isinf(wape_score)
            and not np.isinf(swape_score)
            and abs(wape_score - swape_score) > 0.005
        ):
            if wape_score < swape_score:
                selected_metric = "WAPE"
                res = metric_results["wape"]
                selection_reason = (
                    f"WAPE 誤差率 ({wape_score:.2%}) 小於 sWAPE ({swape_score:.2%})"
                )
            else:
                selected_metric = "sWAPE"
                res = metric_results["swape"]
                selection_reason = (
                    f"sWAPE 誤差率 ({swape_score:.2%}) 小於 WAPE ({wape_score:.2%})"
                )
        else:
            detected_pattern = detect_data_pattern(df_pnl["營利"].values)
            if detected_pattern == "seasonal":
                selected_metric = "sWAPE"
                res = metric_results["swape"]
                selection_reason = (
                    "兩者誤差相近，依數據模式備援判定為季節型 (採用 sWAPE)"
                )
            else:
                selected_metric = "WAPE"
                res = metric_results["wape"]
                selection_reason = (
                    "兩者誤差相近，依數據模式備援判定為趨勢/成長型 (採用 WAPE)"
                )

    best_overall_window = res["best_window"]

    # 擬合最終模型
    recent_pnl = df_pnl.iloc[-best_overall_window:]

    X_final = sm.add_constant(recent_pnl["營收"])
    y_final = recent_pnl["營業成本"]

    final_ols = sm.OLS(y_final, X_final).fit()

    alpha = final_ols.params["const"]
    beta = final_ols.params["營收"]

    # 包裝成 pd.Series 回傳
    model_series = pd.Series(
        {
            "模型": final_ols,
            "固定成本": alpha,
            "變動成本率": beta,
            "變動營利率": 1 - beta,
            "訓練季數": best_overall_window,
            "評估指標": selected_metric,
            "wape": res["best_score"],
            "naive_wape": res["wape_naive"],
            "較無腦預測改善率": res["wape_imp"],
            "選擇原因": selection_reason,
            "最近季度": df_pnl["財報季度"].max(),
        },
        name="OLS_Model_Package",
    )

    return model_series

def 以營收預測次年度營利(預測至次年底各季營收: 'pandas.Series'
                        ,歷史財報: 'pandas.DataFrame') ->  'pandas.Series':
    """
    一、預測項目：預估各季值
    二、預測參數項目：最近歷史值期間
    三、使用歷史財報訓練 OLS 模型，並針對未來各季營收進行營利預測。
    """
    from zhongwen.表 import 表示
    import pandas as pd
    # 1. 訓練模型並取得 OLS 參數
    model_pkg = 取季營收預測營利模型(歷史財報)
    alpha = model_pkg["固定成本"]
    beta = model_pkg["變動成本率"]

    # 2. 複製未來營收資料並計算預測營利
    df_future = 預測至次年底各季營收.copy().to_frame()
    df_future.columns = ['預估每季營收']
    df_future = df_future[df_future.index > 歷史財報.index.max()]
    df_future = df_future.sort_index()
    # 表示(df_future, 顯示索引=True)



    # 預測營業成本 = Alpha + Beta * 未來營收
    df_future['預測營業成本']= alpha + beta * df_future.預估每季營收
    # 預測營利 = 未來營收 - 預測營業成本
    df_future['預測營利'] = df_future.預估每季營收 - df_future.預測營業成本

    # 計算：最近歷史值同比 (YoY)
    最近季度 = model_pkg.最近季度
    最近歷史值去年同季 = 最近季度 - 4
    yh = 歷史財報.營利
    if 最近歷史值去年同季 in yh.index and yh.loc[最近歷史值去年同季] != 0:
        # 適用於毛利率等百分比變動，若是成長率公式可用: (y_歷史.loc[最近季度] - y_歷史.loc[最近歷史值去年同季]) / y_歷史.loc[最近歷史值去年同季]
        # 這裡採用標準增減幅（若本身為百分比，通常看絕對變動或相對變動，此處依常規百分比成長率計算）
        最近歷史值同比 = (yh.loc[最近季度] - yh.loc[最近歷史值去年同季]) / abs(yh.loc[最近歷史值去年同季])
    else:
        最近歷史值同比 = np.nan

    # 計算：首期預估值同比 (YoY)
    yf = df_future.預測營利
    首期預估季度 = yf.index.min()
    首期預估去年同季 = 首期預估季度 - 4
    if 首期預估去年同季 in yh.index and yh.loc[首期預估去年同季] != 0:
        首期預估值同比 = (yf.loc[首期預估季度] - yh.loc[首期預估去年同季]) / abs(yh.loc[首期預估去年同季])
    else:
        首期預估值同比 = np.nan

    model_pkg['預估各季值'] = df_future.預測營利
    model_pkg['最後預估值時間'] = df_future.index.max()
    model_pkg['預估值數量'] = df_future.shape[0]
    model_pkg['歷史值數量'] = 歷史財報.shape[0]
    model_pkg['模型名稱'] = 'OLS'
    model_pkg['最近歷史值同比'] = 最近歷史值同比
    model_pkg['首期預估值同比'] = 首期預估值同比
    model_pkg = model_pkg.rename({'最近季度':'最近歷史值時間'
                                 ,'訓練季數':'回測資料數'
                                 })
    return model_pkg

@通知執行時間
@增加股票分析函數依資料時間更新快取功能(營收預測營利結果快取檔, ['營收月份', '財報季度'])
def 以營收預測次年每股盈餘(股票, 歷月營收表=None):
    '''
    一、以指定股票歷月營收表預測前年至次年每股盈餘。
    二、預測結果：前年至次年每股盈餘、預估說明、預估方法說明。
    三、增加預測上季每股盈餘及實際每股盈餘差異。
    四、歷月營收表最近營收月份或歷季損益表最近財報季度大於快取對應值始更新。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.營收分析 import 取預測盈餘說明, 取歷月營收表
    from twse_crawler.預估次年底 import 表達預估方法, 表達預估說明
    from twse_crawler.營收分析 import 預測次年底營收
    from zhongwen.快取 import 刪除指定名稱快取
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 表示, 數據不足
    from zhongwen.數 import 取最簡約數
    from zhongwen.文 import 臚列
    import pandas as pd
    import zhongwen
    import twse_crawler.營收預測營利模型 as 營收預測營利模型
    公司代號 = 查股票代號(股票)
    公司簡稱 = 查股票簡稱(股票)
    歷季損益表 = 取損益表(公司代號)
    最近財報季度 = 歷季損益表.財報季度.max()
    歷季損益表['營收'] = 歷季損益表.營收.fillna(0)

    # 取最近營收月份
    df = 取歷月營收表(股票)
    最近營收月份 = df.營收月份.max()

    # 快取判斷
    # try:
    #     c = 營收分析快取[f'以營收預測次年每股盈餘預({股票})']
    #     if not zhongwen.快取.停止快取 and (c.最近營收月份 >= 最近營收月份 and c.最近財報季度 >= 最近財報季度):
    #         return c
    # except KeyError: pass

    try:
        最近損益 = 歷季損益表.iloc[-1]
    except IndexError as e:
        raise 數據不足(f'{公司簡稱}歷季損益', 0, 1, '預測前年至次年每股盈餘')
    except Exception as e:
        errmsg = f'{type(e).__name__}({e})'
        m = f"{公司代號}發生{errmsg}"
        raise Exception(m)
    try:
        歷季損益表 = 歷季損益表.set_index(歷季損益表.財報日期.dt.to_period('Q'))
    except AttributeError:
        歷季損益表['財報日期'] = 歷季損益表.index

    from twse_crawler.股利分析 import 無法預估盈餘
    if 歷季損益表.營利.isna().all():
        raise 無法預估盈餘(
                f'{公司簡稱}歷季損益表僅無營利資料，'
                 '無法以營收預測次年每股盈餘！')

    # 計算業外影響程度 
    業外影響程度 = abs(歷季損益表.業外損益.iloc[-1]) / abs(歷季損益表.稅前淨利.iloc[-1])

    # 預測營收
    預估營收結果 = 預測次年底營收(股票)
    最近營收月份 = 預估營收結果.最近歷史值時間
    # 預測營收 = 預估營收結果.預估每季總值.預估每季營收
    預測營收 = 預估營收結果.預估每季總值
    future_index = 預測營收.index[預測營收.index > 歷季損益表.index.max()]
    new_index = 歷季損益表.index.append(future_index)
    歷季損益表 = 歷季損益表.reindex(new_index)
    歷季損益表['營收'] = 歷季損益表.營收.fillna(預測營收)

    # 以營收預測營利
    # from twse_crawler.預估次年底 import 依外部季數據預估次年底數值
    # 預估營利結果 = 依外部季數據預估次年底數值(歷季損益表.營利.dropna()
    #                                          ,歷季損益表[['營收']]
    #                                          ,預估目標='營利', 單位='元')
    # 預測營利方法說明 = 表達預估方法(預估營利結果,'營利')
    # 預測營利 = 預估營利結果.預估各季值

    from twse_crawler.營收預測營利模型 import 以營收預測次年度營利
    from twse_crawler.財報分析 import 取財報彙總表
    預估營利結果 = 以營收預測次年度營利(預測營收, 取財報彙總表(股票))
    歷季損益表['營利'] = 歷季損益表.營利.fillna(預估營利結果.預估各季值)
    預測營利方法說明 = 表達預估方法乙(預估營利結果)
    預測營利 = 預估營利結果.預估各季值

    # 預測業外損益
    from twse_crawler.預估次年底 import 預估至次年底每季值
    預估業外損益結果 = 預估至次年底每季值(歷季損益表.業外損益.dropna())
    預測業外損益 = 預估業外損益結果.預估各季值
    歷季損益表['業外損益'] = 歷季損益表.業外損益.fillna(預測業外損益)

    # 預測稅前淨利
    歷季損益表['稅前淨利'] = 歷季損益表.稅前淨利.fillna(預測營利+預測業外損益)

    # 預測淨利
    歷季損益表['淨利'] = 歷季損益表.淨利.fillna(歷季損益表.稅前淨利*0.8)

    # 預測每股盈餘
    q = 取股票基本資料彙總表(股票)
    股數 = q.股數.iloc[-1]
    歷季損益表['每股盈餘'] = 歷季損益表.每股盈餘.fillna(歷季損益表.淨利/股數)
    from zhongwen.時 import 前年至次年各季末
    前年至次年各季數據 = 歷季損益表.reindex(index=前年至次年各季末.to_period('Q'))
    年度每股盈餘 = 前年至次年各季數據.每股盈餘.resample('Y').sum()

    # 計算累積誤差率
    # 累積誤差率 = (預估營收結果.mape) * (1-業外影響程度)
    # 累積誤差率 += 預估業外損益結果.mape * 業外影響程度

    # 表達預估方法及預估結果
    from twse_crawler.預估次年底 import 移除重覆時間詞
    預估方法說明 = (f'以{預估營收結果.預估方法說明}'
         f'，輸入{表達預估方法乙(預估營利結果, "營利")}'
         f'，與{表達預估方法(預估業外損益結果, "業外損益")}'
         f'，加總之稅前損益'
         f'，扣除最高稅率20％之營所稅之損益'
         f'，再除以{取最簡約數(股數)}股之每股盈餘'
         )
    預估說明 = (f'【營收預測】{預估營收結果.預估說明}'
                f'，【營利預測】{表達預估說明乙(預估營利結果, "營利")}'
                f'，【業外預測】{表達預估說明(預估業外損益結果, "業外損益")}'
                f'，【盈餘預測】{取預測盈餘說明(年度每股盈餘, 前年至次年各季數據)}'
                )
    預測結果 = pd.Series({'前年至次年每股盈餘': pd.Series(年度每股盈餘)
                         ,'預測說明':預估說明
                         ,'預估說明':預估說明
                         ,'預估方法說明':預估方法說明
                         ,'最近財報季度':最近財報季度
                         ,'最近營收月份':最近營收月份
                         })
    # 營收分析快取[f'以營收預測次年每股盈餘預({股票})'] = 預測結果
    return 預測結果

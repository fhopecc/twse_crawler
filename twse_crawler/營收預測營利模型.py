import numpy as np
import optuna
import pandas as pd
import statsmodels.api as sm


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
    """分析歷史損益資料，同時評估 WAPE 與 sWAPE，優先採用誤差最小者；

    計算 OLS 模型的 WAPE 及其相較於 Naive 模型的改善程度。
    回傳包含模型與參數的 pd.Series 物件。
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
            "OLS_WAPE": res["best_score"],
            "Naive_WAPE": res["wape_naive"],
            "WAPE較Naive改善量": res["wape_imp"],
            "選擇原因": selection_reason,
            "最近季度": df_pnl["財報季度"].max(),
        },
        name="OLS_Model_Package",
    )

    return model_series



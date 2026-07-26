def calculate_mase(y_true, y_pred, y_train, seasonal_period=1):
    """計算 MASE / Seasonally MASE (sMASE)"""
    import numpy as np

    y_true = np.array(y_true)
    y_pred = np.array(y_pred)
    y_train = np.array(y_train)

    mae_model = np.mean(np.abs(y_true - y_pred))

    m = seasonal_period
    if len(y_train) <= m:
        m = 1

    naive_errors = np.abs(y_train[m:] - y_train[:-m])
    mae_naive = np.mean(naive_errors)

    if mae_naive == 0 or np.isnan(mae_naive):
        return np.nan

    return mae_model / mae_naive


def detect_data_pattern(y_series, seasonal_period=4):
    """備援機制：自動偵測歷史數據模式，回傳 'seasonal' (季節性) 或 'growth' (成長型/單調趨勢型)"""
    import numpy as np
    import statsmodels.api as sm

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
# 1. 模型訓練與優化函數：回傳 pd.Series
# =====================================================================
def 取季營收預測營利模型(
    歷季財報,
    最小訓練季數=8,
    最大訓練季數=28,
    優化試驗次數=30,
    指定誤差衡量指標=None,
):
    """分析歷史損益資料，同時評估 MASE 與 sMASE，優先採用誤差最小者；

    若無法比對則自動以數據模式備援，尋找最佳歷史視窗並擬合 OLS 模型。

    回傳包含模型與參數的 pd.Series 物件。
    """
    import numpy as np
    import optuna
    import pandas as pd
    import statsmodels.api as sm

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

    if user_metric in ["mase", "smase"]:
        target_metrics = [user_metric]
    else:
        target_metrics = ["mase", "smase"]

    metric_results = {}

    for metric in target_metrics:
        period = 4 if metric == "smase" else 1

        def objective(trial):
            window_size = trial.suggest_int(
                "window_size", actual_min_window, actual_max_window
            )
            scores = []

            for i in range(window_size, n_samples):
                X_train = rev_hist[i - window_size : i]
                y_train = cost_hist[i - window_size : i]

                X_test = rev_hist[i]
                y_test = cost_hist[i]

                X_train_const = sm.add_constant(X_train)
                ols = sm.OLS(y_train, X_train_const).fit()

                pred_cost = ols.params[0] + ols.params[1] * X_test

                score = calculate_mase(
                    y_true=[y_test],
                    y_pred=[pred_cost],
                    y_train=y_train,
                    seasonal_period=period,
                )

                if not np.isnan(score):
                    scores.append(score)

            return np.mean(scores) if len(scores) > 0 else float("inf")

        study = optuna.create_study(direction="minimize")
        study.optimize(objective, n_trials=優化試驗次數)

        metric_results[metric] = {
            "best_window": study.best_params["window_size"],
            "best_score": study.best_value,
        }

    # 判定邏輯：以誤差小者優先
    if user_metric in ["mase", "smase"]:
        selected_metric = user_metric.upper()
        best_overall_window = metric_results[user_metric]["best_window"]
        best_overall_score = metric_results[user_metric]["best_score"]
        selection_reason = "使用者指定指標"
    else:
        mase_score = metric_results["mase"]["best_score"]
        smase_score = metric_results["smase"]["best_score"]

        if (
            not np.isinf(mase_score)
            and not np.isinf(smase_score)
            and abs(mase_score - smase_score) > 0.01
        ):
            if mase_score < smase_score:
                selected_metric = "MASE"
                best_overall_window = metric_results["mase"]["best_window"]
                best_overall_score = mase_score
                selection_reason = (
                    f"MASE 誤差率 ({mase_score:.4f}) 小於 sMASE ({smase_score:.4f})"
                )
            else:
                selected_metric = "SMASE"
                best_overall_window = metric_results["smase"]["best_window"]
                best_overall_score = smase_score
                selection_reason = f"sMASE 誤差率 ({smase_score:.4f}) 小於 MASE ({mase_score:.4f})"
        else:
            detected_pattern = detect_data_pattern(df_pnl["營利"].values)
            if detected_pattern == "seasonal":
                selected_metric = "SMASE"
                best_overall_window = metric_results["smase"]["best_window"]
                best_overall_score = smase_score
                selection_reason = (
                    "兩者誤差相近，依數據模式備援判定為季節型 (採用 sMASE)"
                )
            else:
                selected_metric = "MASE"
                best_overall_window = metric_results["mase"]["best_window"]
                best_overall_score = mase_score
                selection_reason = (
                    "兩者誤差相近，依數據模式備援判定為趨勢/成長型 (採用 MASE)"
                )

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
            "誤差率": best_overall_score,
            "評估指標": selected_metric,
            "選擇原因": selection_reason,
            "最近季度": df_pnl["財報季度"].max(),
        },
        name="OLS_Model_Package",
    )

    return model_series


# =====================================================================
# 2. 推理與預測函數：接受 pd.Series 作為模型輸入
# =====================================================================
def predict_future_ebit(model_series, df_future_monthly):
    """傳入擬合好的模型 pd.Series 與未來月營收，預測未來各季營利。"""
    import pandas as pd

    # 相容 pd.Series 的 Key / 屬性讀取方式
    alpha = model_series["固定成本"]
    beta = model_series["變動成本率"]
    last_hist_q = model_series["最近季度"]

    df_future_m = df_future_monthly.copy()
    df_future_m["YearMonth"] = pd.to_datetime(df_future_m["YearMonth"])
    df_future_m["財報季度"] = df_future_m["YearMonth"].dt.to_period("Q")

    rev_col = "營收" if "營收" in df_future_m.columns else "Revenue"

    df_future_q = (
        df_future_m.groupby("財報季度")[rev_col].sum().reset_index()
    )

    df_future_q = df_future_q[
        df_future_q["財報季度"] > last_hist_q
    ].reset_index(drop=True)

    df_future_q["預估固定成本"] = alpha
    df_future_q["預估變動成本"] = beta * df_future_q[rev_col]
    df_future_q["預估營業總成本"] = (
        df_future_q["預估固定成本"] + df_future_q["預估變動成本"]
    )

    df_future_q["預估營利"] = (
        df_future_q[rev_col] - df_future_q["預估營業總成本"]
    )
    df_future_q["預估營業利益率_%"] = (
        df_future_q["預估營利"] / df_future_q[rev_col]
    ) * 100

    result_df = df_future_q[
        [
            "財報季度",
            rev_col,
            "預估固定成本",
            "預估變動成本",
            "預估營利",
            "預估營業利益率_%",
        ]
    ].rename(columns={rev_col: "預估營收"})

    return result_df

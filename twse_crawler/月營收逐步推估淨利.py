def 自月營收逐步推估淨利(月營收, 營利, 業外損益, 稅前淨利, 淨利):
    '''
    一、報告項目：預估各季值、模型名稱、誤差率、歷史值數量、預估值數量、
                  回測資料數、最近歷史值時間、最後預估值時間、
                  最近歷史值同比、首期預估值同比、各項預估說明
    二、如有月營收，則自月營收時間序列逐步預測淨利，如無則自營利時序逐步預估淨利。
    '''
    import pandas as pd
    from zhongwen.表 import 表示
    from twse_crawler.預估至次年底每月值 import 預估至次年底每月值丙式
    from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值
    from twse_crawler.預估至次年底每季值 import 預估至次年底每季值丙式, 表達預估方法丙, 表達預估說明丙

    if 月營收 is not None: # 自月營收逐步預測淨利
        模型名稱 = '自月營收逐步預測淨利'
        x_forecast = 預估至次年底每月值丙式(月營收)
        x_all = x_forecast.預估每季總值
        最後預估值時間 = x_all.index.max()
        # 計算各季
        y = 淨利[淨利.index > x_all.index.min()]
        最近季度 = y.index.max()
        x = x_all[y.index]
        x_future = x_all[x_all.index > 最近季度]
        外推步數 = x_future.shape[0]
        x1 = 營利[y.index]
        x1_forecast = 以單元迴歸預估至次年底每季值(x, x1, x_future)
    else: # 自營利逐步預估淨利
        模型名稱 = '自營利逐步預測淨利'
        x1 = 營利
        x1_forecast = 預估至次年底每季值丙式(x1)
        y = 淨利
        最近季度 = y.index.max()
        外推步數 = x1_forecast.預估值數量
        最後預估值時間 = x1_forecast.最後預估值時間

    x2 = 業外損益[y.index]
    x2_forecast = 預估至次年底每季值丙式(x2)
    x3 = 稅前淨利[y.index]
    x3_all = x1_forecast.預估各季值+x2_forecast.預估各季值
    x3_future = x3_all[x3_all.index > y.index.max()]
    y_forecast = 以單元迴歸預估至次年底每季值(x3, y, x3_future)
    y_all = y_forecast.預估各季值
    # 最近歷史值同比 = (y_all[最近季度] - y_all[最近季度-4]) / y_all[最近季度-4] 
    # 首期預估值同比 = (y_all[最近季度+1] - y_all[最近季度-3]) / y_all[最近季度-3] 
    # 處理「最近歷史值同比」
    if (最近季度 - 4) in y_all.index and y_all[最近季度 - 4] != 0:
        最近歷史值同比 = (y_all[最近季度] - y_all[最近季度 - 4]) / y_all[最近季度 - 4]
    else:
        最近歷史值同比 = None  # 或 float('nan')

    # 處理「首期預估值同比」
    if (最近季度 - 3) in y_all.index and y_all[最近季度 - 3] != 0:
        首期預估值同比 = (y_all[最近季度 + 1] - y_all[最近季度 - 3]) / y_all[最近季度 - 3]
    else:
        首期預估值同比 = None  # 或 float('nan')
        from twse_crawler.預估至次年底每季值 import 表達預估說明丙

    if 月營收 is not None: # 自月營收逐步預測淨利
        各項預估說明 = (
                f'【月營收預估】{表達預估說明丙(x_forecast, '營收')}'
                f'【月營收預估營利】{表達預估說明丙(x1_forecast, '營利')}'
                f'【業外預估】{表達預估說明丙(x2_forecast, '業外損益')}'
                f'【稅前淨利預估淨利】{表達預估說明丙(y_forecast)}'
                )
    else: # 自營利逐步預估淨利
        各項預估說明 = (
                f'【預估營利】{表達預估說明丙(x1_forecast, '營利')}'
                f'【業外預估】{表達預估說明丙(x2_forecast, '業外損益')}'
                f'【稅前淨利預估淨利】{表達預估說明丙(y_forecast)}'
                )

    return pd.Series({
        "預估各季值": y_forecast.預估各季值,
        "模型名稱": 模型名稱,
        "歷史值數量": len(y),
        "預估值數量": 外推步數,
        "最近歷史值時間": 最近季度,
        "最後預估值時間": 最後預估值時間,
        "最近歷史值同比": 最近歷史值同比,
        "首期預估值同比": 首期預估值同比,
        "各項預估說明": 各項預估說明
    })

def 取自月營收或營利逐步推論較佳者(股票):
    from twse_crawler.財報分析 import 取財報彙總表
    from twse_crawler.營收分析 import 預測次年底營收, 取歷月營收表
    from zhongwen.表 import 表示
    import pandas as pd
    h1 = 取歷月營收表(股票)
    回測月數 = 12
    x = h1.營收
    x_validation = x.iloc[-回測月數:]
    x_train = x.iloc[:-回測月數]
    h2 = 取財報彙總表(股票)
    回測季數 = 4
    y = h2.淨利
    y_validation = y.iloc[-回測季數:]
    y_train = y.iloc[:-回測季數]
    x1_train = h2.營利[y.index]
    x2_train = h2.業外損益[y.index]
    x3_train = h2.稅前淨利[y.index]
    y_forecast1 = 自月營收逐步推估淨利(x_train, x1_train, x2_train, x3_train, y_train)
    y_pred1 = y_forecast1.預估各季值[y_validation.index]
    y_forecast2 = 自月營收逐步推估淨利(None, x1_train, x2_train, x3_train, y_train)
    y_pred2 = y_forecast2.預估各季值[y_validation.index]
    from twse_crawler.無腦預測至次年底每季值 import calc_wmape
    wmape1 = calc_wmape(y_validation.values, y_pred1.values)
    wmape2 = calc_wmape(y_validation.values, y_pred2.values)
    if wmape1 < wmape2:
        return pd.Series({"最佳模型":y_forecast1.模型名稱, "誤差率":wmape1})
    else:
        return pd.Series({"最佳模型":y_forecast2.模型名稱, "誤差率":wmape2})

def 以月營收逐步推估淨利(股票):
    '''
    一、各項預估說明
    '''
    from twse_crawler.財報分析 import 取財報彙總表
    from twse_crawler.營收分析 import 取歷月營收表
    from zhongwen.表 import 表示
    h1 = 取歷月營收表(股票)
    h2 = 取財報彙總表(股票)
    r1 = 取自月營收或營利逐步推論較佳者(股票)
    if r1.最佳模型 == '自月營收逐步預測淨利':
        r = 自月營收逐步推估淨利(h1.營收, h2.營利, h2.業外損益, h2.稅前淨利, h2.淨利)
    else:
        r = 自月營收逐步推估淨利(None, h2.營利, h2.業外損益, h2.稅前淨利, h2.淨利)
    r['誤差率'] = r1.誤差率
    r['回測資料數'] = 4
    r['各項預估說明'] = f'{r.各項預估說明}，整體誤差率{r.誤差率:,.0%}'
    return r

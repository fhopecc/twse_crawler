def 分析護城河(股票):
    '''
    一、資料項目：資料日期、分數、評語。
    '''
    from 股票分析.人工分析 import 取本人股票筆記
    import pandas as pd
    s, m, d = 0, '', pd.NaT
    try:
        n = 取本人股票筆記(股票).iloc[-1].筆記
        if '外銷比重' in n:
            f = 取外銷比重(n)
            if f >= 0.5:
                r = 分析美元兌新台幣匯率() 
                m = f'外銷比重{f:.2%}，且' + r.說明
                s += r.較上季末增減比率/0.01*f
                d = r.交易日 if pd.notna(r.交易日) else pd.NaT
        elif '國外投資比例' in n:
            資料時間, 國外投資比例 = 取國外投資比例(n)
            i = 國外投資比例 
            if 國外投資比例 > 0.1:
                r = 分析美元兌新台幣匯率() 
                m = f'國外投資比例{i:.2%}，且' + r.說明
                s += r.較上季末增減比率/0.01*i
                d = r.交易日 if pd.notna(r.交易日) else pd.NaT
        else:
            return pd.Series()
    except IndexError:
        return pd.Series()
    s = min(s, 5)
    res = pd.Series()
    res['分數'], res['評語'], res['資料日期'] = s, m+f'({s:.0f})', d
    return res

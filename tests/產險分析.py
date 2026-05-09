from zhongwen.庫 import 通知執行時間
from diskcache import Cache
from pathlib import Path
import functools

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)
# cache.clear()

def 取產險公司股票代號(公司):
    from 股票分析.股票基本資料分析 import 查股票代號
    if '富邦' in 公司:
        return 查股票代號('富邦金')
    elif '國泰' in 公司:
        return 查股票代號('國泰金')
    elif '新光' in 公司:
        return 查股票代號('新產')
    elif '新安' in 公司:
        return 查股票代號('裕融')
    elif '和泰' in 公司:
        return 查股票代號('和泰車')
    elif '旺旺' in 公司:
        return 查股票代號('旺旺保')
    elif '華南' in 公司:
        return 查股票代號('華南金')
    elif '泰安' in 公司:
        return 查股票代號('萬海')
    elif '南山' in 公司:
        return 查股票代號('潤泰新')
    elif '第一' in 公司:
        return 查股票代號('第一保')
    elif '臺灣產物' in 公司:
        return 查股票代號('台產')
    elif '兆豐' in 公司:
        return 查股票代號('兆豐金')
    elif '中國信託' in 公司:
        return 查股票代號('中信金')

@functools.cache
@通知執行時間
@cache.memoize('取產險業務概況表', expire=180*24*60*60)
def 取產險業務概況表(股票=None):
    '''
    一、主鍵：公司名稱、期間。
    '''
    from 股票分析.股票基本資料分析 import 查股票代號, 查股票簡稱
    from zhongwen.時 import 取期間, 民國年數
    from zhongwen.html import 取表單數據
    from zhongwen.表 import 顯示, 數據不足
    import pandas as pd
    import requests
    import io

    if 股票:
        股票代號 = 查股票代號(股票)
        股票簡稱 = 查股票簡稱(股票代號)
        df = 取產險業務概況表()
        df = df.query('股票代號==@股票代號')
        if df.empty:
            raise 數據不足(f'{股票簡稱}產險業務概況表', 0, 1) 
        return  df

    url = 'https://ins-info.ib.gov.tw/customer/RPT-07010801.aspx'
    s = requests.session()
    r = s.get(url, verify=False)
    f = 取表單數據(r.text)
    data = f['fields']
    data['DropDownList_BYear'] = '98'
    data['DropDownList_EYear'] = f'{民國年數}'
    r = s.post(url, data, verify=False)
    f = 取表單數據(r.text)
    data = f['fields']
    data['btnDownload'] = '%E4%B8%8B%E8%BC%89'
    r = s.post(url, data, verify=False)
    csv = io.StringIO(r.content.decode('utf8'))
    df = pd.read_csv(csv, encoding='utf8', header=2)
    df['期間'] = df.期間.map(取期間)
    df['股票代號'] = df.公司名稱.map(取產險公司股票代號)

    def 計算成長率(group):
        from sklearn.linear_model import LinearRegression
        import numpy as np
        import pandas as pd
        if len(group) < 2:
            return pd.Series({'市占增減趨勢': np.nan, 'R方': np.nan})

        group = group.sort_values('期間')
        group = group.reset_index()
        # 自變量 X (相對天數), 確保是二維陣列
        X = group[['index']].values
        # 因變量 y (分數)
        y = group['市場占有率(%)'].values

        # 創建並擬合線性回歸模型
        model = LinearRegression()
        model.fit(X, y)

        return pd.Series({
            '市占增減趨勢': model.coef_[0],
            'R方': model.score(X, y)
        })
    df1 = df.groupby('公司名稱').apply(計算成長率)
    df = df.query('期間.dt.year==2024')
    df = df.merge(df1, on='公司名稱')
    df = df.sort_values('市場占有率(%)', ascending=False)
    df['市占排名'] = df['市場占有率(%)'].rank(ascending=False)
    return df

@cache.memoize('載入產險業務概況表')
def 載入產險業務概況表():
    '公司名稱、期間'
    from zhongwen.時 import 取期間
    from zhongwen.表 import 顯示
    from pathlib import Path
    import pandas as pd
    csv = Path(__file__).parent / '文件/產險業務概況表.csv'
    df = pd.read_csv(csv, encoding='utf8', header=2)
    df['期間'] = df.期間.map(取期間)
    return df

@functools.cache
def 分析產險市場():
    '股票代號、分數、說明'
    from zhongwen.表 import 顯示
    df = 下載產險業務概況表()

@functools.cache
@通知執行時間
def 分析產險公司(股票):
    '''
    一、市佔前5名每排前1名加千分。
    二、市佔前5名每排前1名加千分。
    '''
    r = 取產險業務概況表(股票).iloc[-1]
    m = f'產險市占率{r["市場占有率(%)"]:,.2f}，排名第{r.市占排名:,.0f}名'
    s = 0
    s +=  rank*-1000 if (rank:=r.市占排名-6) < 0 else 0
    if r.市占增減趨勢>0:
        m+=f'，每年平均增加{r.市占增減趨勢*100:,.2f}個百分點'
    else:
        m+=f'，每年平均減少{abs(r.市占增減趨勢)*100:,.2f}個百分點'
    s+=r.市占增減趨勢*100*500
    r['分數'] = s
    r['評語'] = m
    return r

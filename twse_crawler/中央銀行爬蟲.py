from pathlib import Path
from diskcache import Cache

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

def 中央銀行資產負債表():
    from zhongwen.file import 抓取
    url = 'https://www.cbc.gov.tw/public/data/OpenData/經研處/EF23M01.csv'
    s = 抓取(url)
    import pandas as pd
    from io import StringIO
    df1 = pd.read_csv(StringIO(s), encoding='cp950')
    df1.set_index('期間')
    url = 'https://www.cbc.gov.tw/public/data/OpenData/經研處/EF25M01.csv'
    s = 抓取(url)
    df2 = pd.read_csv(StringIO(s), encoding='cp950')
    df2.set_index('期間')
    df = df1.merge(df2, on='期間')
    return df

@cache.memoize(tag='爬取央行貼放利率', expire=24*60*60)
def 爬取央行貼放利率():
    '主鍵為調整日期，包含重貼現率、擔保放款融通利率及短期融通利率等欄位。'
    import pandas as pd
    from zhongwen.檔 import 抓取
    url = 'https://www.cbc.gov.tw/tw/lp-370-1-1-60.html'
    strio = 抓取(url, 回傳資料形態='StringIO')
    df = pd.read_html(strio, parse_dates=[1])[0].set_index('調整日期').astype(float).iloc[:,:]/100
    df = df.reset_index()
    return df 

def 爬取消費貸款及建築貸款餘額():
    from zhongwen.file import 抓取
    from zhongwen.text import 刪空格
    from lxml import etree
    from io import BytesIO
    import pandas as pd
    url = 'https://www.cbc.gov.tw/tw/cp-526-1078-7BD41-1.html'
    html = 抓取(url)
    tree = etree.HTML(html)   
    xpath='//a[@title="CONSUMER.xlsx"]/@href'
    xls_url = tree.xpath(xpath)[0]
    xls = 抓取(xls_url, 回傳資料形態='bytes')
    df = pd.read_excel(BytesIO(xls))
    columns = df.iloc[1].fillna(df.iloc[2])
    df.columns = columns.map(刪空格).to_list()
    df = df.iloc[3:]
    df['總額'] = df.消費者貸款 + df.建築貸款
    df1 = df.iloc[:, 1:].apply(lambda r: r/r.iloc[-1], axis=1)
    # breakpoint()
    df1.columns = [f'{c}比率' for c in df1.columns]
    df = pd.concat([df, df1], axis=1)
    return df

from pathlib import Path
from zhongwen.pandas_tools import 可顯示
from zhongwen.batch_data import 通知執行時間
from diskcache import Cache
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

@可顯示
@cache.memoize(tag='爬取殖利率', expire=12*60*60)
def 爬取殖利率(民國年=None):
    '殖利率(112)係爬取112年已公布的殖利率資料'
    from zhongwen.date import 今日
    if not 民國年:
        民國年 = 今日().year-1911
    url = f'https://stock.wespai.com/rate{民國年:03}'
    from zhongwen.file import 抓取
    import pandas as pd
    from io import StringIO
    df = pd.read_html(StringIO(抓取(url)))[0]
    from zhongwen.date import 取日期
    df['除息日'] = df.除息日.map(取日期)  
    df['除權日'] = df.除權日.map(取日期)  
    return df
    
@可顯示
@通知執行時間
def 取最新殖利率資料():
    '索引為代號'
    from zhongwen.date import 今日
    df = 爬取殖利率(今日().year-1911)
    df['配息'] = df.配息.fillna(0)
    from zhongwen.number import 轉數值
    df['現金殖利率'] = df.現金殖利率.map(轉數值).fillna(0)

    def 除權息概述(r):
        import pandas as pd
        from zhongwen.date import 民國日期
        from datetime import date
        m = ''
        if r.配息 > 0:
            m = f'每股配發現金{r.配息:.2f}元' 
            if isinstance(r.除息日, date):
                m += f'，{民國日期(r.除息日, "%M月%d日")}除息'
        if r.配股 > 0:
            m += f'；配發股票{r.配股:.2f}元' 
            if isinstance(r.除權日, date):
                m += f'，{民國日期(r.除權日, "%M月%d日")}除權。'
        return m
    df['除權息概述'] = df.apply(除權息概述, axis=1).fillna('')
    df.set_index('代號', inplace=True)
    df.index = df.index.map(str)
    return df

@可顯示
@cache.memoize(tag='爬取營利指標', expire=12*60*60)
def 爬取營利指標():
    '''至檢股讚選定指標並配賦名稱儲存自訂指標結果(至多選定20個指標)，
嗣能以該網址爬取自訂指標數據，索引為代號即股票代號。
'''
    from zhongwen.file import 抓取
    from zhongwen.number import 連續正負次數
    import logging
    import pandas as pd
    logging.debug('重新至檢股讚抓取股票指標數據。')
    url = 'https://stock.wespai.com/p/68908'
    df  = pd.read_html(抓取(url))[0]
    url = 'https://stock.wespai.com/p/68843'
    _df = pd.read_html(抓取(url))[0]
    del _df['公司']
    df = df.merge(_df, how='left', on='代號')
    def 存貨週轉率增減次數(r):
        l = [r['(季)存貨週轉率(次)'] - r['(季-1)存貨週轉率(次)'] 
            ,r['(季-1)存貨週轉率(次)'] - r['(季-2)存貨週轉率(次)'] 
            ,r['(季-2)存貨週轉率(次)'] - r['(季-3)存貨週轉率(次)'] 
            ,r['(季-3)存貨週轉率(次)'] - r['(季-4)存貨週轉率(次)'] 
            ,r['(季-4)存貨週轉率(次)'] - r['(季-5)存貨週轉率(次)'] 
            ]
        return 連續正負次數(l)
    df['存貨週轉率連續增減次數'] = df.apply(存貨週轉率增減次數, axis=1)

    def 應收帳款週轉率增減次數(r):
        l = [r['(季)應收帳款週轉率(次)'] - r['(季-1)應收帳款週轉率(次)'] 
            ,r['(季-1)應收帳款週轉率(次)'] - r['(季-2)應收帳款週轉率(次)'] 
            ,r['(季-2)應收帳款週轉率(次)'] - r['(季-3)應收帳款週轉率(次)'] 
            ,r['(季-3)應收帳款週轉率(次)'] - r['(季-4)應收帳款週轉率(次)'] 
            ,r['(季-4)應收帳款週轉率(次)'] - r['(季-5)應收帳款週轉率(次)'] 
            ]
        return 連續正負次數(l)

    df['應收帳款週轉率連續增減次數'] = df.apply(應收帳款週轉率增減次數, axis=1)
    def 營利消長次數(r):
        l = [r['(季)營業利益成長率(%)']   
            ,r['(季-1)營業利益成長率(%)']   
            ,r['(季-2)營業利益成長率(%)']   
            ,r['(季-3)營業利益成長率(%)']   
            ,r['(季-4)營業利益成長率(%)']   
            ,r['(季-5)營業利益成長率(%)']   
            ]
        return 連續正負次數(l)
    df['營利消長次數'] = df.apply(營利消長次數, axis=1)
    logging.debug(df.columns)
    df.set_index('代號', inplace=True)
    df.index = df.index.map(str)
    return df

@可顯示
@cache.memoize(tag='爬取股票指標數據', expire=12*60*60)
def 爬取股票指標數據():
    '''至檢股讚選定指標並配賦名稱儲存自訂指標結果(至多選定20個指標)。
嗣能以該網址爬取自訂指標數據。
'''
    print('重新至檢股讚抓取股票指標數據。')
    from zhongwen.file import 抓取
    import pandas as pd
    urls = ['https://stock.wespai.com/p/67799'
           ,'https://stock.wespai.com/p/68093'
           ,'https://stock.wespai.com/p/68231'
           ,'https://stock.wespai.com/p/68237'
           ,'https://stock.wespai.com/p/68346'
           ,'https://stock.wespai.com/p/68843'
           ]
    df = pd.read_html(抓取(urls[0], use_requests=True))[0]
    cs = ['配息', '除息日', '配股', '除權日', '發息日']
    for c in cs:
        del df[c]
    for url in urls[1:]:
        _df = pd.read_html(抓取(url))[0] 
        del _df['公司']
        if '68231' in url:
            cs = ['股價'
                 ,'(季)存貨週轉率(次)'
                 ,'(季-1)存貨週轉率(次)'
                 ,'(季-2)存貨週轉率(次)'
                 ,'(季-3)存貨週轉率(次)'
                 ]
            for c in cs:
                del _df[c]
        if '68346' in url:
            del _df['股價']
        df = df.merge(_df, how='left', on='代號')
    df['代號'] = df.代號.map(str)
    from zhongwen.pandas_tools import show_html
    show_html(df, 顯示筆數=3000)
    return df

def 抓取股票行情頁() -> str:
    url = f'https://stock.wespai.com/lists'
    from zhongwen.file import 抓取
    return 抓取(url)

@可顯示
@cache.memoize(tag='抓取營利率資料', expire=15*24*60*60)
def 抓取營利率資料():
    import pandas as pd
    from zhongwen.file import 抓取
    url = 'https://stock.wespai.com/p/69790'
    df = pd.read_html(抓取(url))[0]
    df.set_index('代號', inplace=True)
    del df['公司']
    import re
    df.rename(columns=lambda n: re.sub('\((4季|EPS|元)\)|\(%\)', '', n), inplace=True)
    df['營業毛利率'] = df.營業毛利率/100
    df['營業利益率'] = df.營業利益率/100
    df['稅前淨利率'] = df.稅前淨利率/100
    df['稅後淨利率'] = df.稅後淨利率/100
    df['3年平均配息率'] = df['3年平均配息率']/100
    df['3年平均配息率'] = df['3年平均配息率'].map(lambda v: 1 if v>1 else v)
    df.index=df.index.map(str)
    return df

if __name__ == '__main__':
    # cache.evict('取最新殖利率資料')
    # 取最新殖利率資料(顯示=True) 
    # 抓取營利率資料(顯示=True)
    # (顯示=True)
    # c = 抓取股票行情頁()
    # from zhongwen.pandas_tools import show_html
    # show_html(df, 顯示筆數=500)
    # 爬取股票指標數據(顯示=True)
    # cache.evict('爬取營利指標')
    from zhongwen.pandas_tools import show_html
    from 股票分析.股票基本資料分析 import 查股票代號
    # cache.clear()
    df = 爬取殖利率()
    df = df.query('公司.str.contains("惠")')
    # df = df.loc[查股票代號("惠普")]
    show_html(df)
    # 取最新殖利率資料(顯示=True)

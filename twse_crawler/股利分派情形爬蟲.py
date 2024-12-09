from zhongwen.庫 import 結果批次寫入
from zhongwen.時 import 自起始年底按年列舉至本年底
from diskcache import Cache
from pathlib import Path
import logging

logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

股利分派情形庫 = Path(__file__).parent.parent / '資料庫' / '股利分派情形庫'

@cache.memoize('抓取股利分派資料', expire=24*60*60)
@結果批次寫入(股利分派情形庫, '股利分派情形彙總表', '股東會召開年度'
             ,自起始年底按年列舉至本年底(100))
def 抓取股利分派情形彙總表(股東會召開年度):
    '''批號為股東會召開年度，以該年度年底表達，
網路公布資料最早為民國100年，故預設爬取自100年迄今年資料。
'''
    from zhongwen.時 import 取日期, 年底, 取期間
    from zhongwen.文 import 刪空格
    from zhongwen.數 import 取數值
    from zhongwen.檔 import 抓取
    import pandas as pd
    logger.info(f"爬取{股東會召開年度.year}年度股東會股利分派資料……")
    url = 'https://mops.twse.com.tw/server-java/t05st09sub'
    資料={"step":1
         ,"TYPEK":"sii"
         ,"YEAR":股東會召開年度.year - 1911
         ,'qryType':1
         } 
    p1 = 抓取(url, 抓取方式='post', 資料=資料, encoding='big5', 回傳資料形態='StringIO')
    資料['TYPEK']="otc"
    p2 = 抓取(url, 抓取方式='post', 資料=資料, encoding='big5', 回傳資料形態='StringIO')

    def to_csv_read_back(df):
        csv = Path(__file__).parent / 'temp.csv'
        df.to_csv(csv)
        return pd.read_csv(csv)
    def 二維欄位重命名(pair):
        n = pair[1]
        if pair[0] != pair[1]:
            n = f'{pair[0]}_{pair[1]}'
        return 刪空格(n)

    dfs = []
    dfs += pd.read_html(p1)[2:]
    dfs += pd.read_html(p2)[2:]
    dfs = [df for df in dfs if df.shape[1] > 1]
    for df in dfs:
        try:
            df.columns = [二維欄位重命名(c) for c in df.columns]
        except:
            # 110 年欄位有問題
            breakpoint()
            df.columns = ['錯誤欄'] + [刪空格(c[1]) for c in df.columns[1:]]
            del df['錯誤欄']
            df = df.query('公司代號名稱.notna()')
    df = pd.concat(dfs)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df['股東會召開年度'] = 股東會召開年度
    first = lambda t: t[0].strip()
    df['股票代號'] = df.公司代號名稱.str.split('-').map(first)
    renamer = {'股東配發內容_盈餘分配之現金股利(元/股)':'盈餘配息'
              ,'股東配發內容_法定盈餘公積、資本公積發放之現金(元/股)':'公積配息'
              ,'股東配發內容_盈餘轉增資配股(元/股)':'盈餘配股'
              ,'股東配發內容_法定盈餘公積、資本公積轉增資配股(元/股)':'公積配股'
              ,'股東配發內容_法定盈餘公積發放之現金(元/股)':'法定公積配息'
              ,'股東配發內容_法定盈餘公積轉增資配股(元/股)':'法定公積配股'
              ,'股東配發內容_資本公積發放之現金(元/股)':'資本公積配息'
              ,'股東配發內容_資本公積轉增資配股(元/股)':'資本公積配股'
              ,'股東配發內容_股東配發之現金(股利)總金額(元)':'現金股利總額'
              ,'股東配發內容_股東配股總股數(股)':'股票股利總股數'
              ,'股東配發內容_股東配股總股數(股)':'股票股利總股數'
              ,'本期淨利(淨損)(元)':'本期淨利'
              }
    df = df.rename(columns=renamer)
    try:
        df['配息'] = df.盈餘配息.fillna(0) + df.公積配息.fillna(0)
        df['配股'] = df.盈餘配股.astype(float).fillna(0) + df.公積配股.fillna(0)
    except AttributeError as e: 
        logger.debug(e) # 110 年度以後資料，公積配息再細分成法定公積配息及資本公積配息
        df['配息'] = df.盈餘配息.fillna(0) + df.法定公積配息.fillna(0) + df.資本公積配息.fillna(0)
        df['配股'] = df.盈餘配股.astype(float).fillna(0) + df.法定公積配股.fillna(0) + df.資本公積配股.fillna(0)
    return df

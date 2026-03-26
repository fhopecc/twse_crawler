from diskcache import Cache
from pathlib import Path

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

@cache.memoize('取公司發行可轉債彙總表', expire=24*60*60)
def 取公司發行可轉債彙總表():
    '''
    一、欄位：機構代碼、發行可轉債。
    '''
    from twse_crawler.櫃買中心爬蟲 import 抓轉交換債發行資料
    df = 抓轉交換債發行資料()
    df = df.groupby('機構代碼')[['債券簡稱']].agg(lambda s: '、'.join(s)).rename(columns={'債券簡稱':'發行可轉債'}).reset_index()
    return df



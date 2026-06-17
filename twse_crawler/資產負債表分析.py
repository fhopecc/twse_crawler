from zhongwen.程式 import 通知執行時間
from zhongwen.快取 import 快取至記憶體
from diskcache import Cache
from twse_crawler import 財報爬蟲
from pathlib import Path
import functools
import logging

logger = logging.getLogger(Path(__file__).stem)

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

@快取至記憶體
@通知執行時間
@cache.memoize('取資產負債表', expire=24*60*60)
def 取資產負債表(股票=None, 個體報表=False):
    '''
    一、資產負債表。
    二、主鍵：股票代號、財報類型、財報日期、財報季度
    三、欄位：資產總計、流動資產合計、現金及約當現金、應收帳款淨額、存貨、
              非流動資產合計、投資、透過其他綜合損益按公允價值衡量之金融資產－非流動、
              無形資產、其他非流動資產、 在建工程、
              負債及權益總計、負債總計、
              流動負債合計、合約負債－流動、客戶保證金專戶、
              應付帳款、本期所得稅負債、其他應付款、
              短期借款、應付短期票券、
              負債準備－流動、租賃負債－流動、其他流動負債、
              非流動負債合計、一年或一營業週期內到期長期負債、
              長期借款、應付公司債、
              股本合計、權益總額、權益總計、
              母公司暨子公司所持有之母公司庫藏股股數（單位：股）
    四、指定參數「股票」，係傳回指定個股之資產負債表，依財報季度索引並排序。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from twse_crawler.損益表分析 import 取近年損益表
    from twse_crawler.現流表分析 import 取近年現流表
    from zhongwen.庫 import 批次載入
    from zhongwen.表 import 表示

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取資產負債表(個體報表=個體報表)
        h = df.query('股票代號==@股票代號')
        h['財報季度索引'] = h.財報季度
        h = h.set_index('財報季度索引')
        return h.sort_index()

    df = 批次載入(財報爬蟲.資產負債資料庫, '資產負債表', '財報日期', '財報日期')
    df = df.drop_duplicates(subset=["股票代號", "財報類型", "財報日期"], keep='first')

    if 個體報表:
        df = df.query("財報類型=='個體'")
    else:
        df = df.query("not 財報類型=='個體'")
    df['權益總計'] = df.權益總計.fillna(df.權益總額)
    df['權益總額'] = df.權益總額.fillna(df.權益總計)
    df['財報季度'] = df.財報日期.dt.to_period('Q')
    return df

def 分析投資(股票):
    '投資占比'
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.數 import 取數值
    import pandas as pd
    import re
    h = 歷季資產負債表 = 取財報彙總表(股票)
    r = h.iloc[-1]
    m= ''
    if r.投資 > r.權益總計:
        r['投資占比'] = r.投資 / r.資產總計	
        m = f'投資大於權益，占比{r.投資占比:.0%}，應分析國外投資現況'
    if m == '':
        return pd.Series()
    r = pd.Series()
    r['分數'] = 0
    r['評語'] = m
    return r

骯髒科目 = ['無形資產', '其他非流動資產', '在建工程']
def 分析骯髒科目(股票):
    '''
    一、骯髒科目指模糊且涵蓋廣的科目，因彈性大，如於附注披露不充分，
        易濫用將不良資產轉入其中，以隱匿財務狀況，
        典型如無形資產、其他非流動資產及在建工程。
    二、欄位：分數(-3~0)、評語
    '''
    from twse_crawler.財報分析 import 取財報彙總表
    from zhongwen.表 import 顯示
    import pandas as pd
    desc, score = '', 0
    h = 歷季資產負債表 = 取資產負債表(股票)
    c = 最近資產負債表 = h.iloc[-1] 
    資產 = c.負債及權益總計 
    dirties = []
    for a in 骯髒科目:
        try:
            if pd.notna(c[a]) and (占比 := c[a]/資產) > 0.1:
                dirties.append(f'{a}占{占比:.0%}')
                score -= 占比/0.3
        except KeyError:
            continue
    if score < 0:
        desc = f'骯髒科目{"、".join(dirties)}，需分析科目附註、懸帳及現金流匹配'
    r = 分析結果 = pd.Series()
    r['分數'] = max(score, -3)
    r['評語'] = desc + f'({r.分數:.0f})'
    return r
            
def 分析負債(股票):
    '''
    一、就表內各科目金額與資產總額比較分析。
    一、財務安全(15)：速動比率(6)、流動比率(4)、負債比率(5)。
    三、結果項目：分數、評語。
    '''
    import pandas as pd
    from zhongwen.表 import 表示
    c = 本期資產負債表 = 取資產負債表(股票).iloc[-1]
    存貨 = c.存貨 if pd.notna(c.存貨) else 0
    負債比率 = c.負債總計 / c.負債及權益總計
    速動比 = (c.流動資產合計-存貨) / c.流動負債合計
    流動比 = c.流動資產合計 / c.流動負債合計
    分數, 評語 = 0, []

    if 速動比 > 1: 
        分數 += min((速動比-1)/1, 6)
        評語 += [f'速動比{速動比:.1f}']

    if 流動比 > 1.5: 
        分數 += min((流動比-1.5)/1, 4)
        評語 += [f'流動比{流動比:.1f}']

    if 負債比率 < 0.4: 
        分數 += min((1-負債比率)/0.1, 5)
        評語 += [f'負債比率{負債比率:.0%}']

    r = pd.Series()
    r['分數'] = 分數
    r['評語'] = '，'.join(評語) + f'({分數:.0f})'
    return r

def 取國外投資比率(財報):
    from zhongwen.文 import 刪除中文字間空白
    from zhongwen.pdf import 取文字
    from zhongwen.智 import 詢問
    content = 取文字(財報) 
    content = 刪除中文字間空白(content)
    q = f'請依據提供財報內容計算公司國外投資比例，財報內容如次：{content}'
    r = 詢問(q)
    return r

def 分析客戶保證金(股票):
    '''
    一、結果項目：評語、分數(0~10)。
    二、客戶保證金利息收入亦受重貼現率影響，評語提示允應分析重貼現率現況影響。
    '''
    from twse_crawler.趨勢分析 import 分析歷季數據增減情形, 年化趨勢評分
    import pandas as pd
    歷史資產負債表 = 取資產負債表(股票)
    s = 分數 = 0
    m = 評語 = ''
    r = 客戶保證金增減情形 = \
            分析歷季數據增減情形(歷史資產負債表
                                ,'客戶保證金專戶'
                                ,'財報日期'
                                ,數據名稱='客戶保證金'
                                )
    
    if not 客戶保證金增減情形.empty:
        if r.同比 > 0:
            s = min(( r.同比*abs(r.連續增減次數) ) / 0.1, 10)
        m = r.說明
    r['評語'] = m + f'({s:.0f})'
    r['分數'] = s
    return r

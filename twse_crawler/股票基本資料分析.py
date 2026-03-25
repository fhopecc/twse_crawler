from zhongwen.快取 import 增加快取時序分析結果
from zhongwen.庫 import 通知執行時間
from diskcache import Cache, Index
from pathlib import Path
import functools
import logging

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

logger = logging.getLogger(Path(__file__).stem)

股票人工分析結果快取檔 = Index(str(Path.home() / '.twse_crawler' / '資料庫' / '股票人工分析結果快取檔'))

def 表達位階(位階):
    '第1序位以【首位】表達、第2序位【次位】表達、第3序位【第3位】表達'
    if 位階==1: return '首位'
    if 位階==2: return '次位'
    return f'第{位階:.0f}位'

def 表達存續期間排名(排名):
    '存續期間長度排名第1名以【最長】表達、第2名【次長】表達、第3名【第3長】表達'
    if 排名==1: return '最長'
    if 排名==2: return '次長'
    return f'第{排名:.0f}長'

def 分析公司產業(股票):
    '''
    一、產業加分：特許經營電信(20)、有線電視(13)
    二、產業排名加分：金融保險業(5)、水泥工業(5)
    三、扣分：發行可轉債(-1)
    '''
    import pandas as pd
    d = 取股票基本資料彙總表(股票).iloc[-1]
    s = 0
    m = []

    if d.普通股每股面額 != 10:
        m += [f'普通股每股面額為{d.普通股每股面額:.0f}元']

    if pd.notna(d.供氣區域):
        # 特許經營天然氣(20)，行政特許一項給予10分，且為民生基礎設施，需求持續一項給予10分。
        s += 20
        m += [f'特許經營供應{d.供氣區域}之公用天然氣(20)']

    if d.公司代號 in ['2412', '3045', '4904']:
        # 特許經營電信業，行政特許一項給予10分，且為民生基礎設施，需求持續一項給予10分(20)。
        s += 20
        m += [f'特許經營電信業(20)']

    if d.公司代號 in ['6184', '6464']:
        # 特許經營有線電視(10)，給予行政特許10分，惟有線電視存有線上影音替代風險，需求持續一項不給分。
        s += 10
        m += [f'特許經營有線電視(10)']

    if d.資本額產業排名 < 3:
        if d.產業類別 in ['金融保險業', '水泥工業']:
            # s += 5
            m += [f'資本額居{d.產業類別}{表達位階(d.資本額產業排名)}']
        else:
            # s += 5
            m += [f'資本額居{d.產業類別}{表達位階(d.資本額產業排名)}']

    if d.成立日期產業排名 < 3:
        m += [f'經營存續期間為{d.產業類別}{表達存續期間排名(d.成立日期產業排名)}']

    if pd.notna(d.發行可轉債):
        # s -= 1
        m += [f'發行{d.發行可轉債}等可轉債，存有股價上升時股權遭轉換稀釋風險']

    r = pd.Series()
    r['評語'] = '，'.join(m)
    r['分數'] = s
    return r

@functools.cache
def 查股票代號(股票, 查不到傳回鍵值=True):
    '''
    一、查指定股票簡稱或代號之股票代號，尚未支援股票全名或別名之查詢。
    二、查無股票代號者，回傳空值或指定傳回股票鍵值。
    '''
    from twse_crawler.公開資訊觀測站爬蟲 import 抓公司基本資料
    import re
    pat = r'\d+'
    if re.match(pat, str(股票)):
        return 股票
    股票名稱 = 股票 
    別名={"FB台50":"富邦台50"
         ,"元大期":'元大期貨'
         ,"台通光電":'台通'
         ,'隆大營':'隆大'
         }
    try:
        別名 = 別名[股票名稱]
        股票名稱 = 別名 
    except: pass
    try:
        m = {'富邦台50':'006208'}
        return m[股票名稱]
    except KeyError:
        df = 抓公司基本資料()
        try:
            row = df.query('公司簡稱==@股票名稱').iloc[-1]
            return row.公司代號
        except IndexError:
            m = f'查無{股票名稱}之股票代號'
            if 查不到傳回鍵值:
                logger.info(m)
                return 股票名稱

@cache.memoize('查股票簡稱', tag='查股票簡稱', expire=180*24*60*60)
def 查股票簡稱(股票代號):
    # from 股票分析.證交所爬蟲 import 證券國際證券辨識號碼一覽表
    # df = 證券國際證券辨識號碼一覽表()
    from twse_crawler.公開資訊觀測站爬蟲 import 抓公司基本資料
    股票代號 = 查股票代號(股票代號)
    df = 抓公司基本資料()
    try:
        return df.query('公司代號==@股票代號').iloc[-1].公司簡稱
    except (KeyError, IndexError) as e:
        人工對照 = {'000147':'台灣摩根'
                   ,'000844':'摩根大通'
                   ,'000144':'美林證券'
                   ,'021021':'永豐期貨'
                   ,'021011':'國泰期貨'
                   ,'026000':'富邦期貨'
                   ,'5869':'元富期貨'
                   ,'智基科':'智基'
                   ,'隆大營':'隆大'
                   ,'新洲':'新洲'
                   ,'006208':'FB台50'
                   }
        try:
            return 人工對照[股票代號]
        except KeyError:
            return 股票代號

@cache.memoize(expire=180*60*60)
def 取上市櫃公用天然氣業彙總表():
    '主鍵係【股票代號】'
    from 股票分析.能源署爬蟲 import 爬取公用天然氣事業供氣區域
    from functools import partial
    df = 爬取公用天然氣事業供氣區域()
    天然氣事業股票代號表 = {
            '大台北區瓦斯公司': '9908',
            '陽明山瓦斯公司': '1767',
            '欣湖天然氣公司': '8379',
            '欣欣天然氣公司': '9918',
            '新海瓦斯公司': '9926',
            '欣泰石油氣公司': '8917',
            '欣桃天然氣公司': '1740',
            '欣中天然氣公司': '1769',
            '欣雲天然氣公司': '1739',
            '欣嘉石油氣公司': '1745',
            '欣高石油氣公司': '9931',
            '欣雄天然氣公司': '8908',
            }
    df['股票代號'] = df.公司名稱.map(lambda n: 天然氣事業股票代號表.get(n, n))
    df.dropna(subset='股票代號', inplace=True)
    df = df.groupby('股票代號')['供氣區域'].apply(
            lambda l: '、'.join(l)
            ).to_frame().reset_index()
    return df

@functools.cache
@通知執行時間
@cache.memoize('取股票基本資料彙總表', expire=24*60*60)
def 取股票基本資料彙總表(股票=None):
    '''
    一、欄位：公司代號、公司簡稱、產業類別、普通股每股面額、已發行普通股數或TDR原發行股數、
        普通股盈餘分派或虧損撥補頻率。
    二、僅含上市、上櫃及興櫃公司。
    三、可指定股票並取得該股票紀錄，未指定股票財傳回全部紀錄。
    '''
    from twse_crawler.公開資訊觀測站爬蟲 import 抓公司基本資料, cache
    from 股票分析.可轉債分析 import 取公司發行可轉債彙總表
    from zhongwen.快取 import 刪除指定名稱快取
    from zhongwen.數 import 取數值
    import pandas as pd
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取股票基本資料彙總表()
        return df.query('公司代號==@股票代號')

    df = 抓公司基本資料() 
    df['資本額產業排名'] = df.groupby('產業類別')['實收資本額(元)'].rank(ascending=False)
    df['成立日期產業排名'] = df.groupby('產業類別')['成立日期'].rank()
    df['已發行普通股數或TDR原發行股數'] = df['已發行普通股數或TDR原發行股數'].map(取數值)

    df1 = 取公司發行可轉債彙總表()
    df = df.merge(df1, how='left', left_on='公司代號', right_on='機構代碼')

    df2 = 取上市櫃公用天然氣業彙總表()
    df = df.merge(df2, how='left', left_on='公司代號', right_on='股票代號')
    df = df.dropna(subset=['公司簡稱'])
    return df

def 取股票詳情連結(股票):
    '''
    股票詳情指錢商品錢部落格之投資筆記，如無則取google 搜尋關鍵字結果。
    '''
    from fhopecc.洄瀾打狗人札記 import 取錢商品錢貼文清單
    簡稱 = 查股票簡稱(股票)
    href = f"https://www.google.com/search?q={簡稱}"
    
    df = 取錢商品錢貼文清單()
    df = df.query('title == @簡稱')
    if not df.empty:
        href = df.iloc[-1].url
    return f'<a href={href} target="_blank">{簡稱}</a>'

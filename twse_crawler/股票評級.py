from diskcache import Cache
from pathlib import Path
from zhongwen.快取 import 快取至記憶體
import zhongwen.快取
import logging
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

def 評級股票(股票, 告示例外=True, 例外為空=False) -> "pandas.Series":
    '''
    一、股票評級結果：報酬率、預測報酬率說明、分數、評語、近一季重大訊息。
    '''
    from twse_crawler.財報分析 import 依資料日期距今月數打折分數, 彙總分析
    from zhongwen.文 import 移除符號分隔值清單重覆子字串項目
    from twse_crawler.重大訊息分析 import 載入近一季重大訊息
    from twse_crawler.行情分析 import 預測報酬率
    from zhongwen.時 import 取民國日期
    from zhongwen.表 import 數據不足
    import pandas as pd
    分數 = 0
    評語 = ''

    護城河分數 = 0
    護城河評語 = ''
 
    評級結果 = pd.Series()
    try:
        預測報酬率結果 = 預測報酬率(股票)
        評級結果 = 預測報酬率結果
    except 數據不足 as e:
        if 例外為空: return pd.Series()
        if 告示例外 and '報酬率' in e.目的:
            raise e
    from twse_crawler.權益報酬率分析 import 分析股東權益報酬率
    from twse_crawler.現流表分析 import 分析現金流
    from zhongwen.數 import 取增減數
    # r = 彙總分析(股票, [分析經營效率, 分析財務安全, 分析成長性, 取風險扣分項])
    try:
        r = 分析股東權益報酬率(股票)
        rc = 分析現金流(股票)
        r['分數'] += rc['分數']
        r['評語'] += f'，現流{取增減數(rc.分數)}分'
    except ValueError:
        r = pd.Series({"分數":0, "評語":"不可知"})
    if not r.empty and len(r.評語) > 0:
        分數 = r.分數
        評語 = r.評語
    r2 = 分析護城河(股票)
    if not r2.empty and len(r2.評語) > 0:
        護城河分數 = r2.分數
        護城河評語 = r2.評語

    edf = 載入近一季重大訊息(股票)
    近一季重大訊息 = "\n".join(
        [f'{取民國日期(r.發言日期)}：{r.主旨}' 
         for r in edf.sort_values('發言日期', ascending=False).itertuples()])
    評級結果['股票'] = 股票
    評級結果['分數'] = 分數
    評級結果['評語'] = 移除符號分隔值清單重覆子字串項目(評語)
    評級結果['護城河分數'] = 護城河分數
    評級結果['護城河評語'] = 移除符號分隔值清單重覆子字串項目(護城河評語)
    評級結果['總分'] = 護城河分數+分數
    評級結果['近一季重大訊息'] = 近一季重大訊息
    return 評級結果

def 分析護城河(股票):
    '''護城河(30%)待實作'''
    from twse_crawler.股票基本資料分析 import 分析公司產業
    from 股票分析.人工分析 import 人工分析護城河
    from twse_crawler.財報分析 import 彙總分析
    r = 彙總分析(股票, [分析公司產業, 人工分析護城河])
    return r
    
def 分析經營效率(股票):
    '''
    一、分析結果：分數、評語。
    二、經營效率：股東權益報酬率20%、現金循環周期10%。
    '''
    from twse_crawler.財報分析 import 依資料日期距今月數打折分數, 彙總分析
    from twse_crawler.財報分析 import 杜邦分析, 分析現金轉換周期
    import pandas as pd
    r = 彙總分析(股票, [杜邦分析, 分析現金轉換周期])
    return r

def 分析財務安全(股票):
    '''
    一、財務安全(15)：速動比率(6)、流動比率(4)、負債比率(5)。
    '''
    from twse_crawler.財報分析 import 彙總分析
    from twse_crawler.資產負債表分析 import 分析負債
    import pandas as pd
    r = 彙總分析(股票, [分析負債])
    return r
 
def 分析成長性(股票):
    '''
    一、分析結果：分數、評語。
    二、成長性(25%)：淨利同比趨勢(5%)、淨利成長領先指標(15%)、自由現金流趨勢(5%)。
    '''
    from twse_crawler.財報分析 import 依資料日期距今月數打折分數, 彙總分析
    from twse_crawler.現流表分析 import 分析自由現金流趨勢
    from twse_crawler.淨利分析 import 分析淨利
    from twse_crawler.營收分析 import 分析月營收
    r = 彙總分析(股票, [分析淨利, 取淨利成長領先指標, 分析自由現金流趨勢])
    return r

def 取淨利成長領先指標(股票):
    '''
    一、分析結果：分數、評語。
    二、淨利成長領先指標(15%)=
        (營利成長領先指標X(1-業外比重)+業外成長領先指標X業外比重+匯率增減情形)X自由現金流對淨利比
    '''
    from twse_crawler.財報分析 import 取自由現金流對淨利比
    from twse_crawler.損益表分析 import 取損益表
    from twse_crawler.營收分析 import 分析月營收
    from twse_crawler.匯率分析 import 分析匯率
    import pandas as pd
    分數 = 0
    評語 = ''

    rs = 取損益表(股票).tail(4) 
    近四季稅前淨利 =  rs.稅前淨利.fillna(0).sum()

    if 近四季稅前淨利 > 0:
        近四季營利 = rs.營利.fillna(0).sum()
        if 近四季營利 < 0:
            近四季業外比重 = 1
        else:
            if 近四季稅前淨利 < 近四季營利:
                近四季業外比重 = 0
            else:
                近四季業外比重 = ( 近四季稅前淨利 - 近四季營利 ) / 近四季稅前淨利 

        r = 取營利成長領先指標(股票) 
        if not r.empty:
            分數 += r.分數 * (1-近四季業外比重)
            評語 += r.評語

        r = 取業外成長領先指標(股票) 
        if not r.empty:
            分數 += r.分數 * 近四季業外比重
            if r.評語:
                評語 += f'；{r.評語}'

        r = 分析匯率(股票)
        if not r.empty:
            分數 += r.分數
            if r.評語:
                評語 += f'；{r.評語}'

    自由現金流對淨利比 = 取自由現金流對淨利比(股票)
    分數 *= min(max(自由現金流對淨利比, 0.1), 1.5)
    評語 += f'，自由現金流對淨利比{自由現金流對淨利比:.1f}'

    s = pd.Series()
    s['分數'] = 分數
    s['評語'] = 評語
    return s

def 取營利成長領先指標(股票):
    '''
    一、指標內容：分數、評語。
    二、營利成長領先指標(15%)：月營收增減情形(5%)、合約負債增減情形(10%)
    '''
    from twse_crawler.財報分析 import 彙總分析, 分析合約負債
    from twse_crawler.營收分析 import 分析月營收
    r = 彙總分析(股票, [分析合約負債, 分析月營收])
    return r

def 取業外成長領先指標(股票):
    '''
    一、指標內容：分數、評語。
    二、業外成長領先指標(15%)：
    三、待實作。
    '''
    from twse_crawler.資產負債表分析 import 分析客戶保證金
    from twse_crawler.財報分析 import 彙總分析
    r = 彙總分析(股票, [分析客戶保證金])
    return r

def 取風險扣分項(股票):
    '''
    一、指標內容：分數、評語。
    二、骯髒科目(-3)
    '''
    from twse_crawler.資產負債表分析 import 分析骯髒科目
    from twse_crawler.財報分析 import 彙總分析
    r = 彙總分析(股票, [分析骯髒科目])
    return r

@cache.memoize('取股票評級彙總表', expire=12*60*60)
def 取股票評級彙總表():
    '''
    一、上市櫃股票評級彙總，評級由報酬率及分數組成。
    二、欄位：公司代號(主鍵)、公司簡稱、產業類別、報酬率、預測報酬率說明、分數、評語、
              前年至次年股利。
    '''
    from twse_crawler.公開資訊觀測站爬蟲 import 抓公司基本資料
    import pandas as pd
    df = 抓公司基本資料()[['公司代號', '公司簡稱','產業類別']]
    # df = df.query('公司代號 in ["1101", "1102", "7749"]')
    def _評級股票(股票):
        try:
            return 評級股票(股票, 告示例外=False)
        except Exception as e:
            raise e
            logger.error(e)
            return pd.Series()
    股票評級結果 = df.公司代號.apply(_評級股票)
    df = df.merge(股票評級結果, how='left', left_on='公司代號', right_on='股票')
    df['分數'] = df.分數.map(lambda n: int(n) if pd.notna(n) else 0)
    return df

def 顯示股票評級彙總表(報酬率下限=0) -> "pandas.DataFrame":
    '''一、傳回取股票評級彙總表。'''
    from twse_crawler.股票基本資料分析 import 取股票詳情連結
    from fhopecc.洄瀾打狗人札記 import 張貼錢商品錢
    from zhongwen.文 import 轉樣式表字串
    from zhongwen.表 import 顯示, 重名加序
    import pandas as pd

    df = 取股票評級彙總表()
    dfo = df.copy()
    df.columns = 重名加序(df.columns)
    顯示欄位 = ['公司簡稱', '分數', '總分'
               ,'報酬率', '產業類別', '護城河分數', '預測報酬率說明'
               ,'護城河評語', '評語', '近一季重大訊息'
               ]
    df = df.query('報酬率>@報酬率下限')
    df["公司簡稱"] = df.公司簡稱.apply(取股票詳情連結)
    df['總分'] = df.總分.fillna(0).map(int)
    df.sort_values(['總分', '報酬率'], ascending=False, inplace=True)
    df = df[顯示欄位]
    樣式, 可顯示資料框 = 顯示(df, 不顯示=True
        ,整數欄位=['總分', '分數', '護城河分數']
        ,百分比欄位=['報酬率']
        ,百分比漸層按值區間欄位=['報酬率']
        ,隱藏欄位=['預測報酬率說明', '護城河評語', '評語', '近一季重大訊息']
        ,顯示索引=False
        ,顯示筆數=3000
        )
    dfv = 可顯示資料框 
    tp = dfv.copy()
    for c in dfv.columns:
        if c == '報酬率':
            tp[c] = dfv.預測報酬率說明.map(轉樣式表字串)
        elif c == '護城河分數':
            tp[c] = dfv.護城河評語+ "。" + dfv.近一季重大訊息
            tp[c] = tp[c].map(轉樣式表字串)
        elif c == '分數':
            tp[c] = dfv.評語+ "。"
            tp[c] = tp[c].map(轉樣式表字串)


    樣式 = 樣式.set_tooltips(tp, [('visibility', 'hidden')
                                 ,('position', 'absolute')
                                 ,('z-index', 1)
                                 ,('transform', 'translate(-20px, -20px)')
                                 ,('background-color', 'black')
                                 ,('color', 'yellow')
                                 ,('display', 'block')
                                 ,('text-align', 'left')
                                 ,('whitespace', 'pre')
                                 ])

    樣式.hide(axis='index')
    內容 = 樣式.to_html()
    顯示(內容)
    標題 = '股票估值榜'
    張貼錢商品錢(標題, 內容, 標籤=['估計股利指標', '股票榜', '股利趨勢指標'])
    return dfo

@快取至記憶體
@cache.memoize('取在線分析結果明細', expire=60*10)
def 取在線分析結果明細(股票=None):
    '''
    一、存放於 github gist 的投資績效
    '''
    from zhongwen.檔 import 抓取
    from twse_crawler.股票基本資料分析 import 查股票簡稱
    import pandas as pd
    if 股票:
        股票 = 查股票簡稱(股票)
        df = 取在線分析結果明細() 
        s = df.query('公司簡稱.str.contains(@股票)').iloc[0]
        s.name = s.公司簡稱
        return s
    url = 'https://gist.githubusercontent.com/fhopecc/fbc1ce4a57f201a0e9e5cc17ddba0fe4/raw/investment_report.json'
    return pd.read_json(url, orient='table')

if __name__ == '__main__':
    from zhongwen.程式 import 列出函數執行時間表
    import zhongwen.快取 
    from twse_crawler.蒐整財務資訊 import 蒐整財務資訊
    import logging
    logging.getLogger('googleapiclient').setLevel(logging.CRITICAL)
    logging.basicConfig(level=logging.INFO)
    # 蒐整財務資訊()
    # zhongwen.快取.停止快取=True
    df = 顯示股票評級彙總表(0.05)
    from 股票分析.投資績效 import 更新在線股票分析結果
    更新在線股票分析結果(df)
    列出函數執行時間表()

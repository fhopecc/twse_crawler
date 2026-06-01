from diskcache import Cache
from pathlib import Path
import functools
import logging
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / 'cache' / Path(__file__).stem)

def 評級股票(股票, 告示例外=True, 例外為空=False):
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
    r = 彙總分析(股票, [分析經營效率, 分析財務安全, 分析成長性, 取風險扣分項])
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

def 顯示股票評級彙總表(報酬率下限=0):
    from twse_crawler.股票基本資料分析 import 取股票詳情連結
    from fhopecc.洄瀾打狗人札記 import 張貼錢商品錢
    from zhongwen.文 import 轉樣式表字串
    from zhongwen.表 import 顯示, 重名加序
    import pandas as pd
    df = 取股票評級彙總表()
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

def 蒐整財務資訊():
    '''
    一、證交所規定上市公司公告申報財務報表之期限如下：
        1.年度財務報告：每會計年度終了後3個月內(3/31前)。
        2.第一季、第二季、第三季財務報告：
          A.一般公司(含投控公司)：每會計年度第1季、第2季及第3季終了後45日內(5/15、8/14、11/14前)。
          B.保險公司：每會計年度第1季、第3季終了後1個月內(4/30、10/31前)，
            每會計年度第2季終了後2個月內(8/31前)。
          C.金控、證券、銀行及票劵公司：每會計年度第1季、第3季終了後45日內(5/15、11/14前)，
            每會計年度第2季終了後2個月內(8/31前)，惟金控公司編製第1季、第3季財務報告時，
            若作業時間確有不及，應於每季終了後60日內(5/30、11/29前)補正。
    '''
    from zhongwen.時 import 今日, 上季, 取季別年數季數, 取日期
    from datetime import timedelta
    from twse_crawler.損益表分析 import cache as bcache
    from twse_crawler.資產負債表分析 import cache as ccache
    from twse_crawler.財報爬蟲 import 爬取上季財報, dcache
    from twse_crawler.現流表分析 import cache as ecache
    from twse_crawler.營收分析 import cache as fcache
    from twse_crawler.自結損益 import cache as gcache
    from twse_crawler.行情分析 import cache as hcache
    from zhongwen.表 import 表示

    # 更新財報收據
    from twse_crawler.資產負債表分析 import 取資產負債表, cache
    from twse_crawler.損益表分析 import 取損益表, cache
    from twse_crawler.現流表分析 import 取現流表
    from twse_crawler.財報爬蟲 import 爬取上季財報 
    from zhongwen.時 import 本季, 上季
    cache.clear()
    季數 = 上季.quarter
    df = 取損益表()
    表示(df.tail(10))
    df = df.groupby('股票代號').agg(財報季度=('財報季度', 'max'))
    df['距今季數'] = df.財報季度.map(lambda q: (本季 - q).n)
    df = df.query('not 距今季數>3')
    for lag_quarter in range(2, 5):
        df_lag = df.query('距今季數==@lag_quarter')
        if not df_lag.empty:
            表示(df_lag, 顯示索引=True)
            from zhongwen.date import 季別
            from zhongwen.時 import 取民國季度
            from twse_crawler.財報爬蟲 import 下載季報包
            from twse_crawler.財報爬蟲 import 爬取資產負債表
            from twse_crawler.財報爬蟲 import 爬取損益表
            from twse_crawler.財報爬蟲 import 爬取現流表
            應更新季度 =  本季 - lag_quarter
            logger.info(f'下載{取民國季度(應更新季度)}財報')
            下載季報包(*季別(應更新季度), 重新下載=True)
            logger.info(f'爬取{取民國季度(應更新季度)}資產負債表')
            爬取資產負債表(應更新季度)
            logger.info(f'爬取{取民國季度(應更新季度)}損益表')
            爬取損益表(應更新季度)
            logger.info(f'爬取{取民國季度(應更新季度)}現流表')
            爬取現流表(應更新季度)
    
    # 更新月營收
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取月營收彙總表
    from twse_crawler.營收分析 import 取歷月營收表, cache
    from zhongwen.時 import 本月
    cache.clear()
    df = 取歷月營收表()
    df = df.groupby('公司代號').agg(營收月份=('營收月份', 'max'))
    df['距今月數'] = df.營收月份.map(lambda m: (本月 - m).n)
    df = df.query('not 距今月數>4')
    for lag_mon in range(2, 5):
        df_lag = df.query('距今月數==@lag_mon')
        if not df_lag.empty:
            表示(df_lag, 顯示索引=True)
            抓取月營收彙總表(本月-lag_mon+1)

    # 更新月自結損益
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表
    from twse_crawler.自結損益 import 取自結損益表, cache
    cache.clear()
    df = 取自結損益表()
    df = df.groupby('公司代號').agg(自結損益月份=('自結損益月份', 'max'))
    df['距今月數'] = df.自結損益月份.map(lambda m: (本月 - m).n)
    df = df.query('not 距今月數>4')
    for lag_mon in range(2, 5):
        df_lag = df.query('距今月數==@lag_mon')
        if not df_lag.empty:
            表示(df_lag, 顯示索引=True)
            抓取月自結損益彙總表(本月-lag_mon+1)

if __name__ == '__main__':
    from zhongwen.程式 import 列出函數執行時間表
    from zhongwen.表 import 顯示
    import zhongwen.快取 
    import logging
    logging.getLogger('googleapiclient').setLevel(logging.CRITICAL)
    logging.basicConfig(level=logging.INFO)
    # cache.clear()
    蒐整財務資訊()
    # zhongwen.快取.停止快取=True
    # 顯示股票評級彙總表(0.05)
    # 列出函數執行時間表()

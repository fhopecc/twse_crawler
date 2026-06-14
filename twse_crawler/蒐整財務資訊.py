from pathlib import Path
import pandas as pd
import logging
logger = logging.getLogger(Path(__file__).stem)

def 蒐整財務資訊(僅顯示落後資訊不予更新=False):
    '''
    一、更新落後2期以上之資訊，如落後2季以上之財報，2月以上之月營收。
    二、排除下市公司，下櫃公司尚待增強。
    三、證交所規定上市公司公告申報財務報表之期限如下：
        1.年度財務報告：每會計年度終了後3個月內(3/31前)。
        2.第一季、第二季、第三季財務報告：
          A.一般公司(含投控公司)：每會計年度第1季、第2季及第3季終了後45日內(5/15、8/14、11/14前)。
          B.保險公司：每會計年度第1季、第3季終了後1個月內(4/30、10/31前)，
            每會計年度第2季終了後2個月內(8/31前)。
          C.金控、證券、銀行及票劵公司：每會計年度第1季、第3季終了後45日內(5/15、11/14前)，
            每會計年度第2季終了後2個月內(8/31前)，惟金控公司編製第1季、第3季財務報告時，
            若作業時間確有不及，應於每季終了後60日內(5/30、11/29前)補正。
    四、每月15日前會先執行抓取上月營收彙總表更新月營收表資料。
    '''
    import twse_crawler.股票基本資料分析
    from zhongwen.時 import 自起日按日列舉迄今, 本季
    from zhongwen.表 import 表示
    import pandas as pd
    下市櫃股票代號 = twse_crawler.股票基本資料分析.取下市櫃股票代號()

    # 更新落後重大訊息
    logger.info('更新落後重大訊息')
    from twse_crawler.重大訊息分析 import 載入近一季重大訊息
    from twse_crawler.重大訊息爬蟲 import 爬取重大訊息
    import requests
    df = 載入近一季重大訊息() 
    最近歸屬日期 = df.歸屬日期.max()
    try:
        for 歸屬日期 in 自起日按日列舉迄今(最近歸屬日期 - pd.Timedelta(days=1)):
            爬取重大訊息(歸屬日期)
    except requests.exceptions.ChunkedEncodingError as e:
        logger.error(f"爬取重大訊息發生{e}！")

    # 更新落後2季以上之財報數據
    logger.info('更新落後2季以上之財報數據')
    from twse_crawler.資產負債表分析 import 取資產負債表
    from twse_crawler.現流表分析 import 取現流表
    twse_crawler.資產負債表分析.cache.clear()
    df = 取資產負債表()
    df = df[~df.股票代號.isin(下市櫃股票代號)]
    df = df.groupby('股票代號').agg(財報季度=('財報季度','max'))
    df = df.query('股票代號=="1101"')
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
            if not 僅顯示落後資訊不予更新:
                應更新季度 =  本季 - lag_quarter + 1
                logger.info(f'下載{取民國季度(應更新季度)}財報')
                下載季報包(*季別(應更新季度), 重新下載=True)
                logger.info(f'爬取{取民國季度(應更新季度)}資產負債表')
                爬取資產負債表(應更新季度)
                logger.info(f'爬取{取民國季度(應更新季度)}損益表')
                爬取損益表(應更新季度)
                logger.info(f'爬取{取民國季度(應更新季度)}現流表')
                爬取現流表(應更新季度)
    
    # 更新落後2月以上之月營收
    logger.info('更新落後2月以上之月營收')
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取月營收彙總表
    from twse_crawler.營收分析 import 取歷月營收表
    from zhongwen.時 import 本月
    twse_crawler.營收分析.cache.clear()
    df = 取歷月營收表()
    df = df[~df.公司代號.isin(下市櫃股票代號)]
    df = df.groupby('公司代號').agg(營收月份=('營收月份', 'max'))
    df['距今月數'] = df.營收月份.map(lambda m: (本月 - m).n)
    df = df.query('not 距今月數>4')
    for lag_mon in range(2, 5):
        df_lag = df.query('距今月數==@lag_mon')
        if not df_lag.empty:
            表示(df_lag, 顯示索引=True)
            if not 僅顯示落後資訊不予更新:
                抓取月營收彙總表(本月-lag_mon+1)

    # 更新月自結損益
    logger.info('更新落後2月以上之自結損益')
    from twse_crawler.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表
    from twse_crawler.自結損益 import 取自結損益表
    twse_crawler.自結損益.cache.clear()
    df = 取自結損益表()
    df = df[~df.公司代號.isin(下市櫃股票代號)]
    df = df.groupby('公司代號').agg(自結損益月份=('自結損益月份', 'max'))
    df['距今月數'] = df.自結損益月份.map(lambda m: (本月 - m).n)
    df = df.query('not 距今月數>4')
    for lag_mon in range(2, 5):
        df_lag = df.query('距今月數==@lag_mon')
        if not df_lag.empty:
            表示(df_lag, 顯示索引=True)
            if not 僅顯示落後資訊不予更新:
                抓取月自結損益彙總表(本月-lag_mon+1)

def 取股票資料最近時間(股票) -> "pandas.Series":
    '''
    一、項目：最近財報季度、最近自結損益月份及最近營收月份。
    二、無該資料即無項目，如無最近自結損益月份，結果即無該項目，
        指定項目會發生 KeyError。
    '''
    from twse_crawler.資產負債表分析 import 取資產負債表
    from twse_crawler.營收分析 import 取歷月營收表
    from twse_crawler.自結損益 import 取自結損益表
    import pandas as pd
    最近資料時間 = pd.Series()
    最近資料時間['最近財報季度'] = 取資產負債表(股票).財報季度.max()
    最近資料時間['最近自結損益月份'] = 取自結損益表(股票).自結損益月份.max()
    最近資料時間['最近營收月份'] = 取歷月營收表(股票).營收月份.max()
    return 最近資料時間.dropna()

def 增加股票分析函數依資料時間更新快取功能(快取檔: "diskcache.Index", 資料時間項目):
    '''
    一、股票分析函數具一個參數股票，並對參數指定股票進行分析。
    二、股票分析函數函數係分析指定股票之分析函數。
    一、依指定取時序函數、名稱欄位、時間欄位、快取檔案增加分析函數快取時序分析結果功能。
    三、名稱即為快取鍵，通常為股票名稱或公司名稱。
    四、時間欄位可為多個，其中一個時戳落後即予更新。
    五、如指定名稱之時序資料為空，則引發數據不足錯誤。
    '''
    from zhongwen.表 import 數據不足
    from zhongwen.時 import 取正式民國日期
    from functools import wraps
    import pandas as pd
    def 取可依資料時間更新快取之股票分析函數(股票分析函數):
        @wraps(股票分析函數)
        def 可依資料時間更新快取之股票分析函數(股票):
            '''
            一、股票分析函數具一個參數股票，並對參數指定股票進行分析。
            '''
            from collections.abc import Iterable 
            from zhongwen.表 import 表示
            import pandas as pd
            import zhongwen.快取
            最近資料時間 = 取股票資料最近時間(股票)
            if not zhongwen.快取.停止快取:
                try:
                    快取 = 快取檔[f'{股票分析函數.__name__}({股票})']
                    if all(快取[f'最近{i}'] >= 最近資料時間[f'最近{i}'] for i in 資料時間項目):
                        logger.info(f'{股票分析函數.__name__}({股票})->快取值！')
                        return 快取
                except KeyError: pass
            r = 股票分析函數(股票)
            快取檔[f'{股票分析函數.__name__}({股票})'] = r
            return r 
        return 可依資料時間更新快取之股票分析函數
    return 取可依資料時間更新快取之股票分析函數

if __name__ == '__main__':
    蒐整財務資訊(僅顯示落後資訊不予更新=True) 

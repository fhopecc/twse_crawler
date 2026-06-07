import pandas as pd
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
    from zhongwen.時 import 今日, 上季, 取季別年數季數, 取日期
    from datetime import timedelta
    from zhongwen.表 import 表示
    import twse_crawler
    下市櫃股票代號 = twse_crawler.股票基本資料分析.取下市櫃股票代號()
    # 更新落後2季以上之財報數據
    from twse_crawler.資產負債表分析 import 取資產負債表
    from twse_crawler.現流表分析 import 取現流表
    from zhongwen.時 import 本季, 上季
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

if __name__ == '__main__':
    蒐整財務資訊(僅顯示落後資訊不予更新=True) 

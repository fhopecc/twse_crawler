from zhongwen.庫 import 結果批次寫入, 增加定期更新, 通知執行時間
from zhongwen.表 import 可顯示
from zhongwen.時 import 自指定季別迄上季
from functools import cache
from diskcache import Cache
from pathlib import Path
from fhopecc import env
import logging

logger = logging.getLogger(Path(__file__).stem)
dcache = Cache(Path.home() / 'cache' / Path(__file__).stem)
ns = {'ix':'http://www.xbrl.org/2013/inlineXBRL'}
存放區:Path = env.datadrive / 'tifrs'
下載區 = 存放區 / 'downloads'

# dcache.clear()
資產負債資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '資產負債資料庫'
原始資產負債資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '原始資產負債資料庫'
損益資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '損益資料庫'
原始損益資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '原始損益資料庫'
現流表資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '現流表資料庫'
原始現流表資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '原始現流表資料庫'
單季損益庫 = Path.home() / '.twse_crawler' / '資料庫' / '單季資料庫'
權益變動資料庫 = Path.home() / '.twse_crawler' / '資料庫' / '權益變動資料庫'

資產負債表使用欄位=['股票代號', '財報類型'
                   ,'資產總計'
                   ,'流動資產合計', '現金及約當現金', '應收帳款淨額', '存貨'
                   ,'非流動資產合計', '投資'
                   ,"透過其他綜合損益按公允價值衡量之金融資產－非流動"
                   ,'負債及權益總計'
                   ,'負債總計'
                   ,'流動負債合計', '合約負債－流動', "客戶保證金專戶"
                   ,'應付帳款', '本期所得稅負債', '其他應付款' 
                   ,'負債準備－流動', '租賃負債－流動', '其他流動負債' 
                   ,'非流動負債合計'
                   ,'無形資產', '其他非流動資產',  '在建工程'
                   ,'股本合計'
                   ,'權益總額', '權益總計', '權益' # 權益總額是權益總計之舊名稱
                   ,'母公司暨子公司所持有之母公司庫藏股股數（單位：股）'
                   ]

損益表使用欄位=['股票代號'
               ,'財報類型'
               ,'收入合計'
               ,'營業收入合計'
               ,'營業利益（損失）'
               ,'營業毛利（毛損）'
               ,'營業利益'
               ,'本期稅前淨利（淨損）'
               ,'本期淨利（淨損）'
               ,'母公司業主（淨利／損）'
               ,'淨收益'
               ,'收益合計'
               ,'稀釋每股盈餘合計'
               ,'基本每股盈餘合計'
               ,'繼續營業單位稅前淨利（淨損）'
               ,'本期稅後淨利（淨損）'
               ,'營業外收入及支出合計'
               ,'營業成本合計'
               ,'銷貨成本'
               ,'利息淨收益'
               ,'手續費淨收益'
               ,'手續費及佣金淨收益'
               ,'保險業務淨收益'
               ,'投資性不動產損益'
               ,'透過損益按公允價值衡量之金融資產及負債損益'
               ,'透過其他綜合損益按公允價值衡量之金融資產已實現損益'
               ,'兌換損益'
               ,'採用權益法認列之關聯企業及合資損益之份額'
               ,'保費收入合計'
               ]

現流表使用欄位=['股票代號'
               ,'財報類型'
               ,'營運產生之現金流入（流出）'
               ,'營業活動之淨現金流入（流出）'
               ,'取得不動產、廠房及設備'
               ,'投資活動之淨現金流入（流出）'
               ,'籌資活動之淨現金流入（流出）'
               ,'短期借款增加','短期借款減少','發行公司債','償還公司債'
               ,'舉借長期借款', '償還長期借款', '其他借款增加', '其他借款減少'
               ,'匯率變動對現金及約當現金之影響'
               ,'本期現金及約當現金增加（減少）數'
               ]

權益變動表使用欄位=['股票代號'
                   ,'財報類型'
                   ,'期末餘額-普通股股本'
                   ,'期末餘額-待分配股票股利'
                   ,'期末餘額-股本合計'
                   ]

def 取數值(nf):
    '''
    一、取財報數值，因財報金額單位係千元，故整數則乘一千以轉為元表達，小數以原數表達。
    '''
    from zhongwen.數 import 取數值
    n = 取數值(nf.text)
    if isinstance(n, int):
        n *= 1000
        n = n*-1 if nf.get('sign')=='-' else n
        return n 
    if isinstance(n, float):
        n = n*-1 if nf.get('sign')=='-' else n
        return n

def 應公布財報季數(股票代號:int or str='1101') -> (str, int, int):
    '''傳回指定公司最近一次公布之財報即上季財報之股票代號、年數及季別等三元數組，前開數組稱為上季財報識別碼。
股票：指定數字證券代碼或股票簡稱之證券。
證交所規定上市公司公告申報財務報表之期限如下：
一、年度財務報告：每會計年度終了後3個月內(3/31前)。
二、第一季、第二季、第三季財務報告：
(一)一般公司(含投控公司)：每會計年度第1季、第2季及第3季終了後45日內(5/15、8/14、11/14前)。
(二)保險公司：每會計年度第1季、第3季終了後1個月內(4/30、10/31前)，每會計年度第2季終了後2個月內(8/31前)。
(三)金控、證券、銀行及票劵公司：每會計年度第1季、第3季終了後45日內(5/15、11/14前)，每會計年度第2季終了後2個月內(8/31前)。惟金控公司編製第1季、第3季財務報告時，若作業時間確有不及，應於每季終了後60日內(5/30、11/29前)補正。'''
    if not 日期: 日期 = 今日()
    no = 取股票代號(股票代號)
    年 = 日期.year
    q1 = date(年, 5, 15)
    q2 = date(年, 8, 14)
    q3 = date(年, 11, 14)
    q4 = date(年, 3, 31)
    if q4 <= 日期 < q1 :
        年 = 年-1
        季 = 4
    elif q1 <= 日期 <= q2:
        季 = 1
    elif q2 < 日期 <= q3:
        季 = 2
    elif q3 <= 今日():
        季 = 3
    elif 今日() < q4:
        年 = 年-1
        季 = 3
    return no, 年, 季 

@cache
def 分類檔():
    'tifrs 分類檔(zip格式)'
    import requests as r
    c = r.get('https://emops.twse.com.tw/nas/taxonomy/e_taxonomies.html')
    zips =  re.findall(r'href="(.+\.zip)"', c.text)
    return zips

def 下載分類檔():
    def 分類檔網址(_zip):
        return f'https://emops.twse.com.tw/nas/taxonomy/{_zip}'
    urls = [(分類檔網址(_zip), 下載區 / _zip) for _zip in 分類檔()]
    for url, _zip in urls:
        if not _zip.exists():
            下載(url, _zip)
            time.sleep(10)

def 解壓分類檔():
    zips = list(下載區.glob(r'**/tifrs-*.zip'))
    from stock.tifrs import root
    for z in zips:
        try:
            解壓(z, root())
        except:
            print(f'fails to unzip {z}!')

@cache
def 季報檔():
    '公開資訊觀測站公布之上市櫃季報彙總檔()'
    from zhongwen.檔 import 抓取
    import re
    url = 'https://mopsov.twse.com.tw/mops/web/t203sb02'
    c = 抓取(url)
    fs = re.findall('fileName=([^&]*)&', c)
    return fs

def 季報檔網址(fn):
    import re
    m = re.match(r'[^\d]*(([0-9A]{4,6})Q(\d)).*', fn)
    yqstr = m.group(1)
    year = int(m.group(2))
    quarter = int(m.group(3))
    url = "https://mopsov.twse.com.tw/server-java/FileDownLoad?step=9&"
    return url + f'fileName={fn}&filePath=/home/html/nas/ifrs/{year}/'

def 下載季報包(年或年季=None, 季=None, 重新下載=False, 覆寫=False):
    '預設係自網站抓取全部最近應公布財報季'
    from zhongwen.檔 import 下載, 解壓
    from zhongwen.時 import 取期間, 上季
    年 = 年或年季
    if not (年 or 季):
        指定季度 = 上季
    elif 年 and not 季:
        指定季度 = 取期間(f'{年}')
    elif 年 and 季:
        指定季度 = 取期間(f'{年}Q{季}')

    年 = 指定季度.year
    季 = 指定季度.quarter
    logger.info(f'下載{年}年第{季}季財報彙總檔……')
    覆寫 = 重新下載 
    urls = [(季報檔網址(p), 下載區 / p) for p in 季報檔()]

    urls = [url for url in urls if f'{年}Q{季}' in url[0]]
    for url, p in urls:
        if 重新下載 or not p.exists():
            try:
                z = 下載(url, p, 覆寫=重新下載)
                q = z.stem[-6:]
                if 重新下載 or sum(1 for _ in 存放區.glob(f'*{指定季度}*'))+10 < (sum(1 for _ in 存放區.glob(f'*{指定季度-2}*'))):
                    解壓(z, 存放區)
                    logger.info(f"解壓{list(存放區.glob(f'*{指定季度}*'))}")
                else:
                    logger.info(f'{z.name}己解壓！')

            except FileNotFoundError as e:
                if not p.parent.exists():
                    p.parent.mkdir(parents=True)
                    return 下載季報包(年, 季, 覆寫)
                raise e
                
def 財報匯總檔(年數, 季別):
    urls = [(季報檔網址(p), 下載區 / p) for p in 季報檔()]
    return [url for url in urls if f'{年}Q{季}' in url[0]][1]

def 取財報檔(股票, 季別或年數=None, 季數=None):
    '''
    一、以代號或簡稱指定股票。
    二、預設為上季財報，如無則傳回最近公布財報。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from zhongwen.時 import 上季, 取期間
    from collections import deque
    def 取季別年數季數(q):
        q = 取期間(q)
        return q.year, q.quarter
    股票代號 = 查股票代號(股票)
    股票簡稱 = 查股票簡稱(股票)
    if 季數:
        年數 = 季別或年數
    elif 季別或年數:
        季別 = 季別或年數
        年數, 季數 = 取季別年數季數(季別)
    else:
        年數, 季數 = 取季別年數季數(上季)
    
    try:
        html = next(存放區.glob(f"*{股票代號}*{年數}Q{季數}.html"))
    except Exception:
        logger.warn(f'{股票簡稱}尚未公布{年數}Q{季數}財報檔！')
        html = deque(存放區.glob(f"*{股票代號}*.html"), 1)[0] # 運用 deque 取最後一個元素
    return html

@cache
@dcache.memoize('爬取上季財報', expire=24*60*60)
def 爬取上季財報(占位無功能參數=1):
    from zhongwen.date import 季別, 前幾季
    from zhongwen.時 import 上季
    from zhongwen.庫 import 批次寫入
    import sqlite3
    logger.info('爬取上季財報')
    下載季報包(*季別(上季), 重新下載=True)
    logger.info('爬取資產負債表')
    爬取資產負債表(上季)
    logger.info('爬取損益表')
    爬取損益表(上季)
    logger.info('爬取現流表')
    爬取現流表(上季)

def 財報期間字串(財報日期):
    return f'From{財報日期.year}0101To{財報日期:%Y%m%d}'

def 財報單季字串(財報日期):
    from zhongwen.date import 季初
    return f'From{季初(財報日期):%Y%m%d}To{財報日期:%Y%m%d}'

def 取基本資料(ixbrl:bytes):
    '回傳財報之股票代號、類型、日期)'
    from zhongwen.時 import 取期間
    import re
    pat = r'<title>\s*(.+) (\d{4}Q\d) Financial report'.encode('utf8')
    m = re.search(pat, ixbrl)
    股票代號, 財報日期 = m[1].decode(), 取期間(m[2]).end_time.normalize()
    pat = r'\d{4}年第\d季(合併|個體|個別)財務報告'.encode('utf8')
    m = re.search(pat, ixbrl)
    財報類型 = m[1].decode()
    return 股票代號, 財報類型, 財報日期
 
def 取資產負債表(財報檔):
    from zhongwen.number import 轉數值
    from zhongwen.時 import 取期間
    from lxml import etree
    import pandas as pd
    import socket
    import math
    import re
    if isinstance(財報檔, Path):
        ixbrl = 財報檔.read_bytes()
    else:
        ixbrl = 財報檔
    r = etree.HTML(ixbrl)   
    # 取基本資料
    xpath = '/html/head/title'
    title = r.xpath(xpath)[0].text.strip()
    xpath = '//div[@id="BalanceSheet"]/following-sibling::table[1]//tr[position()>=3]'
    trs = r.xpath(xpath)
    ns = [tr.xpath('.//span[@class="zh"]')[0].text.strip() for tr in trs]
    vs = [tr.xpath('.//td[3]//nonfraction') for tr in trs]
    # if socket.gethostname().upper() in ['LAPTOP-KSFQ396P', 'ELAO1K-113', 'DESKTOP-4DMM4CJ']:
    #     vs = [tr.xpath(
    #         './/td[3]//*[local-name()="ix:nonfraction"]') for tr in trs]
    # else:
    #      s = [tr.xpath(
    #         './/td[3]//*[local-name()="nonfraction"]') for tr in trs]

    vs = [tr.xpath(
     f'.//td[3]//*[local-name()="ix:nonfraction" or local-name()="nonfraction"]'
         ) for tr in trs]

 
    vs = [轉數值(v[0].text)*1000 if v else float('nan') for v in vs]
    s = pd.Series(vs, ns).dropna()
    s[s.index.str.contains('單位：股')] /= 1000 
    # 取基本資料
    xpath = '/html/head/title'
    title = r.xpath(xpath)[0].text.strip()
    pat = r'.*?(\w+)\s(\d+Q\d)\s.*'
    if m:=re.match(pat, title):
        s['股票代號'] = m[1]
        s['財報日期'] = 取期間(m[2]).end_time.normalize()
    else:
        breakpoint()
        raise ValueError(f'標題{title}不含財報資訊')

    xpath='//*[local-name()="nonnumeric" and contains(@name, "tifrs-notes:ReportCategory")]'
    xpath='//*[contains(@name, "tifrs-notes:ReportCategory")]'
    w = r.xpath(xpath)
    財報類型 = r.xpath(xpath)[0].text.replace('\r', '').replace('\n', '')
    財報類型 = {"Consolidated report":'合併'
               ,"Entity report":'個體'
               ,"Individual report":'個別'
               }[財報類型]

    s['財報類型'] = 財報類型

    duplicated_index = s.index.duplicated(keep='first')
    # 使用布林索引選擇第一次出現的索引
    s_unique = s[~duplicated_index]
    df = s_unique.to_frame().T.groupby(level=0).first()
    return df 

def 取損益表(財報檔, 單季=False, 改取現流表=False):
    '''
    因損益表及現流表皆表達各科目1季度之流量，解析財報檔邏輯相似，
    故增加「改取現流表」選項，俾重覆運用程式邏輯。
    '''
    from zhongwen.number import 轉數值
    from zhongwen.表 import 重名加序
    from zhongwen.時 import 取期間
    from lxml import etree
    import pandas as pd
    import socket
    import math
    import re

    if isinstance(財報檔, Path):
        ixbrl = 財報檔.read_bytes()
    else:
        ixbrl = 財報檔

    r = etree.HTML(ixbrl)   
    namespace = r.nsmap

    # 取基本資料
    xpath = '/html/head/title'
    title = r.xpath(xpath)[0].text.strip()
    pat = r'.*?(\w+)\s(\d+Q\d)\s.*'
    財報日期 = pd.NaT
    if m:=re.match(pat, title):
        股票代號 = m[1]
        財報日期 = 取期間(m[2]).end_time.normalize()
    else:
        raise ValueError(f'標題{title}不含財報資訊')
    # print(ixbrl)
    # xpath='//*[local-name()="nonnumeric" and contains(@name, "tifrs-notes:ReportCategory")]'
    xpath='//*[contains(@name, "tifrs-notes:ReportCategory")]'
    財報類型 = r.xpath(xpath)[0].text.replace('\r', '').replace('\n', '')
    財報類型 = {"Consolidated report":'合併'
               ,"Entity report":'個體'
               ,"Individual report":'個別'
               }[財報類型]

    # 取損益表數據
    xpath = '//div[@id="StatementOfComprehensiveIncome"]/following-sibling::table[1]//tr[position()>=3]'
    if 改取現流表:
        xpath = '//div[@id="StatementsOfCashFlows"]/following-sibling::table[1]//tr[position()>=3]'
    trs = r.xpath(xpath)
    ns = [tr.xpath('.//span[@class="zh"]')[0].text.strip() for tr in trs]
    統計期間字串 = 財報期間字串(財報日期)
    if 單季:
        統計期間字串 = 財報單季字串(財報日期)
    # if socket.gethostname().upper() in ['LAPTOP-KSFQ396P', 'ELAO1K-113'
                                       # ,'DESKTOP-4DMM4CJ', 'LAPTOP-H5COA']:
        # vs = [tr.xpath(
        # f'.//*[local-name()="ix:nonfraction" and @contextref="{統計期間字串}"]'
        # ) for tr in trs]
    # else:
    vs = [tr.xpath(
     f'.//*[ (local-name()="ix:nonfraction" or local-name()="nonfraction") and @contextref="{統計期間字串}"]'
         ) for tr in trs]

    vs = [取數值(v[0]) if v else float('nan') for v in vs]
    s = pd.Series(vs, ns).dropna()
    s['股票代號']=股票代號
    if 單季:
        s['財報季別'] = pd.Period(財報日期, freq='Q')
    else:
        s['財報日期'] = 財報日期
    s['財報類型']=財報類型
    df = s.to_frame().T.groupby(level=0).first()
    df.columns = 重名加序(df.columns)
    return df 

def 取現流表(財報檔, 單季=False):
    return 取損益表(財報檔, 單季, 改取現流表=True)

def 取權益變動表(ixbrl:str):
    from zhongwen.pandas_tools import 重名加序
    from lxml import etree
    import pandas as pd
    import socket
    import math
    import re
    股票代號, 財報類型, 財報日期 = 取基本資料(ixbrl)
    r = etree.HTML(ixbrl)   
    namespace = r.nsmap
    xpath = '//div[@id="StatementsOfChangeInEquity"]/following-sibling::table[1]//tr'
    trs = r.xpath(xpath)
    xpath = './/span[@class="zh"]'
    try:
        表頭 = trs[2].xpath(xpath)
    except IndexError:
        return pd.DataFrame()

    表頭 = [span.text for span in 表頭]
    資料 = trs[3:]
    def 取紀錄(tr):
        xpath = './/span[@class="zh"]'
        表側 = tr.xpath(xpath)[0].text.strip()
        if socket.gethostname().upper() in ['LAPTOP-KSFQ396P', 'ELAO1K-113', 'DESKTOP-4DMM4CJ']:
            xpath = './/*[local-name()="ix:nonfraction"]'
        else:
            xpath = './/nonfraction'
        數值 = tr.xpath(xpath)
        數值 = [取數值(v) for v in 數值]
        return [表側, *數值]
    紀錄集 = [取紀錄(tr) for tr in 資料]
    df = pd.DataFrame(紀錄集, columns = ['項目', *表頭])
    df = df[df.項目.str.contains("餘額")]
    df = df.set_index('項目')
    s = df.stack()
    s.index = s.index.map(lambda ns: '-'.join(ns))
    股票代號, 財報類型, 財報日期 = 取基本資料(ixbrl)
    s['股票代號']=股票代號
    s['財報日期']=財報日期
    s['財報類型']=財報類型
    df = s.to_frame().T.groupby(level=0).first()
    if not df.columns.is_unique:
        df.columns = 重名加序(df.columns)
    return df 

def 讀取財報資料(htmls, 財報解析函數):
    import pandas as pd
    import aiofiles
    import asyncio
    async def 異步讀取財報資料(htmls):
        async def 異步讀取單筆財報資料(html):
            async with aiofiles.open(html, mode='rb') as file:
                content = await file.read()
                return 財報解析函數(content)
        if not isinstance(htmls, list): 
            htmls = [htmls]
        tasks = []
        for html in htmls:
            task = asyncio.create_task(異步讀取單筆財報資料(html))
            tasks.append(task)
        dfs = await asyncio.gather(*tasks)
        df = pd.concat(dfs, ignore_index=True)
        return df 
    return asyncio.run(異步讀取財報資料(htmls))

def 平行讀取財報資料(年, 季, 財報解析函數):
    '爬取100個檔案約費時3.97秒。'
    from zhongwen.pandas_tools import 分割鏈
    from multiprocessing import Pool 
    from functools import partial
    import pandas as pd
    import math
    import socket
    htmls = [html for html in 存放區.glob(f"*{年}Q{季}.html")]
    if len(htmls) == 0:
        return pd.DataFrame()
    核心數 = 4
    if socket.gethostname() == 'DESKTOP-4DMM4CJ':
        核心數 = 12
    每核處理量 = math.ceil(len(htmls)/核心數)
    subhtmls = 分割鏈(htmls, 每核處理量)
    p = Pool()
    dfs = p.map(partial(讀取財報資料, 財報解析函數=財報解析函數), subhtmls)
    p.close()
    p.join()
    df = pd.concat(dfs, ignore_index=True)
    df = df.reset_index(drop=True)
    return df

@通知執行時間
@結果批次寫入(資產負債資料庫, '資產負債表', '財報日期', 指定欄位=資產負債表使用欄位)
def 爬取資產負債表(季末):
    from zhongwen.date import 季別
    年, 季 = 季別(季末)
    logger.info(f'爬取{年}年第{季}季資產負債表……')
    return 平行讀取財報資料(年, 季, 取資產負債表)

@通知執行時間
@結果批次寫入(損益資料庫, '損益表', '財報日期', 指定欄位=損益表使用欄位)
def 爬取損益表(季末):
    '取累計損益表'
    from zhongwen.date import 季別
    年, 季 = 季別(季末)
    logger.info(f'爬取{年}年第{季}季累計損益表……')
    return 平行讀取財報資料(年, 季, 取損益表)

@通知執行時間
@結果批次寫入(現流表資料庫, '現流表', '財報日期', 指定欄位=現流表使用欄位)
def 爬取現流表(季末):
    '爬取現流表'
    from zhongwen.時 import 取季別年數季數
    年, 季 = 取季別年數季數(季末)
    logger.info(f'爬取{年}年第{季}季現流表……')
    return 平行讀取財報資料(年, 季, 取現流表)

@通知執行時間
@結果批次寫入(權益變動資料庫, '權益變動表', '財報日期', 指定欄位=權益變動表使用欄位)
def 爬取權益變動表(季末):
    from zhongwen.date import 季別
    年, 季 = 季別(季末)
    logger.info(f"爬取{年}年第{季}季權益變動表……")
    return 平行讀取財報資料(年, 季, 取權益變動表)

def 爬取財報(季別):
    '2019Q1後財報為iXBRL格式'
    from zhongwen.時 import 取期間
    logger.info(f'爬取第{季別}財報')
    季別 = 取期間(季別)
    爬取損益表(季別)
    爬取資產負債表(季別)
    爬取現流表(季別)
    # 爬取權益變動表(季別)

@通知執行時間
@dcache.memoize(tag='載入權益變動表', expire=6*60*60)
@增加定期更新('財報', 更新程序=爬取上季財報)
def 載入權益變動表():
    '主鍵為股票代號、財報類型及財報日期'
    from zhongwen.batch_data import 載入批次資料
    df = 載入批次資料(權益變動資料庫, '權益變動表', '財報日期', '財報日期')
    df = df.set_index(['股票代號', '財報類型', '財報日期'])
    return df

def 爬取網頁式財報():
    '2019Q1後財報為iXBRL格式，自開始上傳'
    from zhongwen.date import 取日期, 迄每季
    # 分兩批避免讀檔競爭
    下載季報包() 
    for 季 in 迄每季(取日期('20190101')):
        logger.info(f'爬取{季}損益表')
        爬取損益表(季)

    for 季 in 迄每季(取日期('20190101')):
        logger.info(f'爬取{季}資產負債表')
        爬取資產負債表(季)

def 爬取商業標記語言財報():
    '2014至2018年度財報為XBRL格式'
    raise NotImplementedError('爬取商業標記語言財報尚待實作')

def 爬取公開資訊觀測站彙總表():
    '2013年度以前財報資料'
    raise NotImplementedError('爬取商業標記語言財報尚待實作')
 
def 取股票合併財報公司(ixbrl:str):
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(ixbrl, 'html.parser')
    attrs = {'name':"tifrs-notes:CompanyID" }
    股票代號 = soup.find('ix:nonnumeric', attrs=attrs).text 

    attrs = {'name':"tifrs-notes:CompanyChineseName" }
    母公司 = soup.find('ix:nonnumeric', attrs=attrs).text 

    attrs = {'name':"tifrs-notes:TheConsolidatedEntities"}
    ts = soup.find_all('ix:tuple', attrs=attrs)
    attrs = {'name':"tifrs-notes:NameOfInvestee"}
    ns = [母公司]
    ns += [t.find('ix:nonnumeric', attrs=attrs).text for t in ts]
    return {'股票代號':股票代號, '公司名稱':ns}

async def 併行取股票合併財報公司(htmls):
    async def 自檔案取股票合併財報公司(html):
        import aiofiles
        async with aiofiles.open(html, mode='r', encoding='utf8') as file:
            content = await file.read()
            return 取股票合併財報公司(content)
    import asyncio
    tasks = []
    for html in htmls:
        task = asyncio.create_task(自檔案取股票合併財報公司(html))
        tasks.append(task)
    df = await asyncio.gather(*tasks)
    import pandas as pd
    df = pd.DataFrame(df)
    return df

def 執行併行取股票合併財報公司(htmls):
    # 100 files paring use 47.61s
    import asyncio
    return asyncio.run(併行取股票合併財報公司(htmls))

@可顯示
def 平行取股票合併財報公司(年=2023, 季=1):
    '''
100個檔每8個由8個內核各自併行處理，費時15.64秒
100個檔每13個由8個內核各自併行處理，費時11.55秒
100個檔每12個由8個內核各自併行處理，費時8.4秒
800個檔由8個平行處理每個併行處理200個檔，費時73.04秒，不可以大於100個檔，會閒置部分核心
800個檔由8個平行處理每個併行處理100個檔，費時58秒
800個檔由8個平行處理每個併行處理50個檔，費時60.71秒
800個檔由8個平行處理每個併行處理8個檔，費時64.00秒
'''
    from zhongwen.pandas_tools import 分割鏈
    from multiprocessing import Pool 
    from math import floor
    import pandas as pd
    htmls = [html for html in 存放區.glob(f"*{年}Q{季}.html")]
    p = Pool()
    核心數 = 8
    每核處理量 = floor(len(htmls)/核心數)
    subhtmls = 分割鏈(htmls, 每核處理量)
    df = p.map(執行併行取股票合併財報公司, subhtmls)
    p.close()
    p.join()
    df = pd.concat(df, ignore_index=True)
    df = df.explode('公司名稱')
    return df

@可顯示
@dcache.memoize(tag='取股票合併財報公司資料', expire=365*24*60*60)
def 取股票合併財報公司資料():
    df = 平行取股票合併財報公司(*應公布財報季別())
    from zhongwen.text import 公司正名
    df['公司名稱'] = df.公司名稱.map(公司正名)
    df.reset_index(drop=True, inplace=True)
    df.index = df.index+1
    df.index.name = '編號'
    return df

def 爬取自一零八年迄今之現流表():
    from zhongwen.時 import 自指定季別迄上季, 取期間
    for 季 in 自指定季別迄上季(取期間('108年第1季')):
        爬取現流表(季)

def 爬取自一零八年迄今之損益表():
    from zhongwen.時 import 自指定季別迄上季, 取期間
    for 季 in 自指定季別迄上季(取期間('108年第1季')):
        爬取損益表(季)

def 爬取財報電子書(公司代號或簡稱, 年數=None, 季數=None):
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.財報爬蟲 import 財報單季字串 
    from zhongwen.batch_data import 解析更新期限
    from zhongwen.檔 import 下載, 抓取
    from zhongwen.date import 季別
    from zhongwen.pdf import 解鎖
    from lxml import etree
    import pandas as pd
    公司代號 = 查股票代號(公司代號或簡稱)
    if pd.isna(年數) or pd.isna(季數):
        財報資料時期, _, _ = 解析更新期限('財報')
        年數, 季數 = 季別(財報資料時期)
    try:
        local_dir = Path(__file__).parent / '電子書'
        return next(local_dir.glob(f'{年數}{季數:02}_{公司代號}_*.pdf'))
    except StopIteration:
        url = f'https://doc.twse.com.tw/server-java/t57sb01?co_id={公司代號}&colorchg=1&kind=A&step=9&filename={年數}{季數:02}_{公司代號}_AI1.pdf'
        c = 抓取(url, return_bytes=True)
        t = etree.HTML(c)
        pdf = t.xpath('//a')[0].get('href')
        pdfurl = f'https://doc.twse.com.tw{pdf}'
        f = 下載(pdfurl, 儲存目錄=Path(__file__).parent / '電子書')
        # 解鎖(f)
        return f

def 顯示財報檔(公司, 季別=None):
    import os
    f = 取財報檔(公司, 季別) 
    os.system(f"start {f}")

def 爬取年報檔(公司, 年數=None):
    '回傳本地年報檔路徑'
    from twse_crawler.股票基本資料分析 import 查股票代號
    from zhongwen.檔 import 抓取, 下載
    from zhongwen.時 import 今年數
    from io import StringIO
    import requests
    import re
    import pandas as pd

    公司代號 = 查股票代號(公司)
    if pd.isna(年數):
        年數 = 今年數-1911-1
    url = f"https://doc.twse.com.tw/server-java/t57sb01?co_id={公司代號}&year={年數}&colorchg=1&step=1&mtype=F"
    # html = StringIO(requests.get(url).text)
    html: StringIO = 抓取(url, 回傳資料形態='StringIO', encoding='cp950')
    年報檔 = 檔名 = pd.read_html(html)[1].query('資料細節說明=="股東會年報"').iloc[0].電子檔案
    data = {"co_id":公司代號
           ,"filename":年報檔 
           ,"colorchg":1
           ,"step":9
           ,"kind":"F"
           ,}
    url = f"https://doc.twse.com.tw/server-java/t57sb01"
    html = 抓取(url, 'post', 參數=data, encoding='cp950')
    print(html)
    # requests.post(url, data=data).text
    pat = r"電子檔案：<a href='/pdf/(\w+.pdf)'>"
    m=re.search(pat, html)
    pdf = m[1]
    url = f"https://doc.twse.com.tw/pdf/{pdf}"
    return 下載(url, 儲存目錄=Path(__file__).parent / '電子書')

def 以原始資產負債資料庫重建資產負債資料庫():
    '主要係修正使用欄位重建使用'
    from zhongwen.表 import 顯示
    import pandas as pd
    import sqlite3
    with sqlite3.connect(原始資產負債資料庫) as c:
        sql = f"select * from 資產負債表"
        df = pd.read_sql_query(sql, c, index_col='index') 
        print(list(df.columns))
        資產負債表使用欄位.append('財報日期')
        df = df[資產負債表使用欄位]
        with sqlite3.connect(資產負債資料庫) as c2:
            df.to_sql('資產負債表', c2, if_exists='replace')

def 整理財報資料表():
    '刪除股票代號為空的紀錄'
    import sqlite3
    import re
    with sqlite3.connect(資產負債資料庫) as conn:
        sql = f'DELETE FROM 資產負債表 WHERE 股票代號 IS NULL'
        c = conn.cursor()
        c.execute(sql)
        conn.commit()

    with sqlite3.connect(損益資料庫) as conn:
        sql = f'DELETE FROM 損益表 WHERE 股票代號 IS NULL'
        c = conn.cursor()
        c.execute(sql)
        conn.commit()
        sql = f'DELETE FROM 損益表 WHERE 基本每股盈餘合計=0'
        c = conn.cursor()
        c.execute(sql)
        conn.commit()

def 以原始損益資料庫重建損益資料庫():
    '主要係修正使用欄位重建使用'
    from zhongwen.表 import 顯示
    import pandas as pd
    import sqlite3
    with sqlite3.connect(原始損益資料庫) as c:
        sql = f"select * from 損益表"
        df = pd.read_sql_query(sql, c, index_col='index') 
        print(list(df.columns))
        損益表使用欄位.append('財報日期')
        df = df[損益表使用欄位]
        with sqlite3.connect(損益資料庫) as c2:
            df.to_sql('損益表', c2, if_exists='replace')

def 重建現流資料庫():
    '主要係修正使用欄位重建使用'
    from zhongwen.表 import 顯示
    import pandas as pd
    import sqlite3
    with sqlite3.connect(原始現流表資料庫) as c:
        sql = f"select * from 現流表"
        df = pd.read_sql_query(sql, c, index_col='index') 
        print(list(df.columns))
        現流表使用欄位.append('財報日期')
        df = df[現流表使用欄位]
        with sqlite3.connect(現流表資料庫) as c2:
            df.to_sql('現流表', c2, if_exists='replace')
 
def 複製檔案至剪貼簿(檔案):
    import win32clipboard
    import win32con
    win32clipboard.OpenClipboard()
    win32clipboard.EmptyClipboard()

    # 準備 CF_HDROP 格式的資料（檔案路徑用雙 null 結尾）
    filepath_bytes = (str(檔案) + '\0').encode('utf-16le') + b'\0'
    win32clipboard.SetClipboardData(win32con.CF_HDROP, filepath_bytes)
    win32clipboard.CloseClipboard()

if __name__ == '__main__':
    import argparse
    import os
    parser = argparse.ArgumentParser()
    parser.add_argument("stock", nargs='?', type=str, help="指定股票")
    parser.add_argument("-t", "--tifrs", action="store_true", help="顯示指定股票財報檔")
    parser.add_argument("-a", "--annual", action="store_true", help="顯示指定股票年報")
    parser.add_argument("-r", "--rebuild_balance", action="store_true", 
                        help="以原始資產負債資料庫重建資產負債資料庫")
    parser.add_argument("-c", "--rebuild_cashflow", action="store_true", 
                        help="重建現流資料庫")
    parser.add_argument("-q", "--quarter", type=str, help="指定股票財報季別，形式為2025Q2")
    args = parser.parse_args()
    if args.tifrs:
        if args.stock:
            f = 取財報檔(args.stock)
            cmd = f'start {f}'
            os.system(cmd) 
    elif args.annual:
        if args.stock:
            f = 爬取年報檔(args.stock)
            cmd = f'start {f}'
            print(cmd)
            os.system(cmd) 
    elif args.rebuild_balance:
        以原始資產負債資料庫重建資產負債資料庫()
    elif args.rebuild_cashflow:
        重建現流資料庫()
    elif q:=args.quarter:
        下載季報包(q, 重新下載=True, 覆寫=True)
        # 爬取財報(q)

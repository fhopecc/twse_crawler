from pathlib import Path
import logging
logger = logging.getLogger(Path(__file__).stem)
失敗重爬次數 = 0
def 爬取重大訊息(日期=None):
    '爬取當日重大訊息'
    global 失敗重爬次數 
    from zhongwen.date import 取日期, 民國日期, 今日, 年底
    from zhongwen.file import 抓取
    from zhongwen.text import 刪空格
    from zhongwen.number import 轉數值
    from lxml import etree
    import pandas as pd
    import time
    import re
    日期 = 取日期(日期)
    logger.info(f"爬取{民國日期(日期)}重大訊息……")
    url = 'https://mops.twse.com.tw/mops/web/ajax_t05st02'
    資料  = 'encodeURIComponent=1&step=1&step00=0&firstin=1&off=1&TYPEK=all'
    資料 += f'&year={日期.year-1911}&month={日期.month:02}&day={日期.day:02}'
    html = 抓取(url, 抓取方式='post', 資料=資料, encoding='utf8')
    tree = etree.fromstring(html, etree.HTMLParser())
    try:
        table = tree.xpath('//table')[2]
    except IndexError as e:
        失敗重爬次數 += 1
        if 失敗重爬次數 > 2:
            raise RuntimeError(f'重爬{民國日期(日期)}重大訊息2次仍失敗！')
        else:
            logger.info('因應網站防爬機制，暫停5秒重爬……')
            time.sleep(5)
            return 爬取重大訊息(日期)
    trs = table.xpath('tr')
    def 取記錄(tr):
        ths = tr.xpath('th')
        if len(ths) == 0:
            def 取值(td):
                s = 刪空格(td.xpath('string()'))
                if s == '': #
                    inputs = td.xpath(".//input[@type='hidden']")
                    # content = ';'.join(刪空格(i.get('value', '')) for i in inputs)
                    # content = content.replace('1;;1;1;;;;t05st02;', '')
                    # content = content.replace(';詳細資料', '')
                    return inputs[-1].get('value')
                return s
            return [取值(td) for td in tr.xpath('td')]
        return [th.xpath('string()') for th in ths]
    tds = [取記錄(tr) for tr in trs]
    header = tds[0]
    df = pd.DataFrame(tds[1:], columns=header)
    df = df.query('發言日期!="發言日期"')
    df['發言日期'] = df.發言日期.map(取日期)
    df.loc[:, '歸屬日期'] = 日期
    df = df.rename(columns={'':'訊息'})
    return df

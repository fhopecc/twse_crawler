from pathlib import Path
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

def 建置測例(股票):
    from twse_crawler.重大訊息爬蟲 import 抓取重大訊息詳細資料
    from 股票分析.重大訊息分析 import 載入近一季重大訊息 
    from 股票分析.股票基本資料分析 import 查股票簡稱
    from zhongwen.number import 中文數字
    from zhongwen.date import 取日期, 季別, 上季
    from zhongwen.表 import 顯示
    from pathlib import Path
    import requests
    import json
    股票簡稱 = 查股票簡稱(股票)
    季末 = 上季()
    _, 季數 = 季別(季末)
    df = 載入近一季重大訊息()
    df = df.query('公司名稱==@股票簡稱')
    df = df.query(f'主旨.str.contains(".*(通過)?.*第({中文數字(季數)}|{季數})季.*財務報告.*(通過)?")')

    訊息 = json.loads(df.iloc[-1].詳細資料)
    for i in range(5):
        try:
            訊息 = 抓取重大訊息詳細資料(訊息)['result']['data'][-1][-1]
            break
        except requests.exceptions.HTTPError:
            continue
    f = Path(__file__).parent / f'{股票簡稱}財務報告公告.txt'
    f.write_text(訊息)

def 讀取測例(股票簡稱):
    from zhongwen.date import 取日期
    from pathlib import Path
    import pandas as pd
    import re
    f = Path(__file__).parent / f'{股票簡稱}財務報告公告.txt'
    return f.read_text()

class Test(unittest.TestCase):

    def test載入近一季重大訊息(self):
        from twse_crawler.重大訊息分析 import 載入近一季重大訊息
        from zhongwen.表 import 顯示
        df = 載入近一季重大訊息()
        # df = df.query('公司名稱=="合邦"')
        顯示(df)

    def test取本季重大訊息歸戶表(self):
        from twse_crawler.重大訊息分析 import 取本季重大訊息歸戶表, cache
        from 股票分析.股票基本資料分析 import 查股票代號
        from zhongwen.表 import 顯示
        from zhongwen.快取 import 刪除指定名稱快取
        # 刪除指定名稱快取(cache, '載入近一季重大訊息')
        df = 取本季重大訊息歸戶表()
        df = df.query('公司代號==@查股票代號("泰銘")')
        顯示(df)

    def test解析耀登公告財務報告(self):
        from twse_crawler.重大訊息分析 import 解析公告財務報告
        公告 = 讀取測例('耀登') 
        logger.info(公告)
        r = 解析公告財務報告(公告)
        self.assertEqual(r['營收'],      742037000)
        self.assertEqual(r['毛利'],      293397000)
        self.assertEqual(r['營利'],           6000)
        self.assertEqual(r['稅前淨利'],  -38211000)
        self.assertEqual(r['淨利'],      -42782000)
        self.assertEqual(r['母公司淨利'],-41514000)
        self.assertEqual(r['基本每股盈餘'], -0.83)

    def test探勘公告財務報告表(self):
        from twse_crawler.重大訊息分析 import 探勘公告財務報告表, 公告財務報告表欄位, cache
        from 股票分析.股票基本資料分析 import 查股票代號
        from 股票分析.財報爬蟲 import 損益資料庫 
        from zhongwen.表 import 顯示
        from zhongwen.庫 import 批次載入
        # cache.clear()
        df = 探勘公告財務報告表()
        # df = df.query('公司代號==@查股票代號("漢唐")')
        顯示(df)

        for c in 公告財務報告表欄位:
            self.assertIn(c, df.columns)

        # dfset = df.公司代號+ df.財報日期.astype(str)+df.財報類型.map(
                            # {"合併":'N', "個別":"N", "個體":"Y"})
        # 顯示(dfset, 顯示筆數=3000)

    def test取公告股利彙總表(self):
        from twse_crawler.重大訊息分析 import 取公告股利彙總表, cache
        from zhongwen.表 import 顯示
        # cache.clear()
        df = 取公告股利彙總表()
        # df = df.query('公司名稱=="華碩"')
        顯示(df)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    # 建置測例('耀登')
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test載入近一季重大訊息'))
    # suite.addTest(Test('test取本季重大訊息歸戶表'))
    # suite.addTest(Test('test解析耀登公告財務報告'))
    # suite.addTest(Test('test探勘公告財務報告表'))
    # suite.addTest(Test('test取公告股利彙總表'))
    unittest.TextTestRunner().run(suite)

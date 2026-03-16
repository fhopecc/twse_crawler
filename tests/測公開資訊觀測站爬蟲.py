from unittest.mock import patch
from pathlib import Path
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):
    def test抓取月營收彙總表(self):
        from twse_crawler.公開資訊觀測站爬蟲 import 抓取月營收彙總表
        from zhongwen.表 import 表示
        import pandas as pd
        df = 抓取月營收彙總表(pd.Period('2026-02'))
        表示(df)

    def test抓取公司股利分派公告資料彙總表(self):
        from 股票分析.公開資訊觀測站爬蟲 import 抓取公司股利分派公告資料彙總表
        from zhongwen.表 import 顯示 
        from zhongwen.時 import 今日 
        import pandas as pd
        df = 抓取公司股利分派公告資料彙總表(今日)
        顯示(df)

    def test抓取月自結損益彙總表(self):
        from 股票分析.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表
        from zhongwen.表 import 顯示 
        import pandas as pd
        df = 抓取月自結損益彙總表(pd.Period('2025-07')) 
        顯示(df)

    def test抓公司基本資料(self):
        from twse_crawler.公開資訊觀測站爬蟲 import 抓公司基本資料, cache
        from zhongwen.表 import 顯示, 檢核欄位資料型態是否為字串
        from 股票分析.股票基本資料分析 import 查股票代號
        from zhongwen.快取 import 刪除指定名稱快取
        # 刪除指定名稱快取(cache, '抓公司基本資料')
        df = 抓公司基本資料()
        數字代號 = 查股票代號('數字')
        df2 = df.query('公司代號==@數字代號')
        顯示(df2)
        self.assertTrue(檢核欄位資料型態是否為字串(df, '公司代號'))

        # 須排除興櫃公司
        df1 = 興櫃公司 = df.query("上市櫃日期.isna() and 興櫃日期.notna()")
        顯示(df1)
        self.assertTrue(興櫃公司.empty)

        # 須排除存託憑證
        df1 = 存託憑證 = df.query("產業類別=='存託憑證'")
        # 顯示(df1)
        self.assertTrue(存託憑證.empty)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test抓取月營收彙總表'))
    # suite.addTest(Test('test抓取公司股利分派公告資料彙總表'))
    # suite.addTest(Test('test抓取月自結損益彙總表'))
    # suite.addTest(Test('test抓公司基本資料'))
    unittest.TextTestRunner().run(suite)

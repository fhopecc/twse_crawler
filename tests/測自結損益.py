from unittest.mock import patch
from pathlib import Path
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test(self):
        from twse_crawler.自結損益 import 以自結營利預測次年每股盈餘
        from twse_crawler.自結損益 import 以自結損益預測次年每股盈餘
        from twse_crawler.自結損益 import 取自結損益表, cache
        from zhongwen.表 import 數據不足, 表示
        from twse_crawler.公開資訊觀測站爬蟲 import 抓取月自結損益彙總表, 自結損益資料庫
        r = 以自結營利預測次年每股盈餘('遠傳')
        表示(r, 顯示索引=True, 顯示筆數=1000)

        self.assertTrue(False)
        # 豐興2013年12月有個別及合併2筆紀錄，刪除個別紀錄。
        cache.clear()
        df = 取自結損益表()
        df = df.query("公司簡稱 == '豐興' and 本月合併稅前損益.isna()")
        self.assertTrue(df.empty)

        表示(r, 顯示索引=True, 顯示筆數=1000)
        self.assertEqual(len(r), 2)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營業損益、業外損益及稅前損益')

        with self.assertRaises(數據不足兩筆): 預測前年至次年每股盈餘('數字')

        r = 預測前年至次年每股盈餘('崑鼎')
        # 顯示(r)
        self.assertEqual(len(r), 2)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營業損益、業外損益及稅前損益')

        r = 預測前年至次年每股盈餘('台灣高鐵')
        # 顯示(r)
        self.assertEqual(len(r), 2)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營業損益、業外損益及稅前損益')

        r = 預測前年至次年每股盈餘('富邦金')
        顯示(r)
        self.assertEqual(len(r), 2)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '稅前損益')


    def test預測前年至次年周期數據(self):
        from twse_crawler.自結損益 import 以自結損益預測次年每股盈餘
        from zhongwen.表 import 表示
        預測結果 = 以自結損益預測次年每股盈餘('鈊象')
        表示(預測結果)
        self.assertFalse(True)


    def test載入自結損益表(self):
        from 股票分析.自結損益 import 載入自結損益表, cache
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 顯示
        from zhongwen.時 import 本月
        # 刪除指定名稱快取(cache, '載入自結損益表')
        df = 載入自結損益表('3682') # 已下市之亞太電，資料只到2020年12月
        self.assertTrue(df.empty)

    def test分析歷月數據增減情形(self):
        from 股票分析.自結損益 import 分析歷月數據增減情形
        from zhongwen.時 import 取日期, 今日
        from zhongwen.表 import 顯示
        import pandas as pd
        df = 讀取測例('崑鼎')
        # 顯示(df)
        df['業外損益'] = df.本月合併稅前損益 - df.本月合併營業損益
        r = 分析歷月數據增減情形(df
                                  ,'業外損益'
                                  ,'自結損益月份'
                                  )
        for c in ['同比', '歷史增加排名', '本月', '去年同月']:
            self.assertIn(c, r.index)
        print(r)

    def test分析月稅前損益(self):
        from twse_crawler.自結損益 import 分析月稅前損益
        from zhongwen.表 import 表示
        import pandas as pd
        r = 分析月稅前損益('元大期', 重新分析=True)
        表示(r)

if __name__ == '__main__':
    import pandas as pd
    import logging
    import warnings
    pd.options.mode.chained_assignment = None 
    # warnings.filterwarnings("ignore", category=FutureWarning)
    # warnings.filterwarnings("ignore", category=DeprecationWarning)
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # 建構自結損益表測例()
    # unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test預測前年至次年周期數據'))  
    # suite.addTest(Test('test分析歷月數據增減情形'))  
    # suite.addTest(Test('test載入自結損益表'))  
    suite.addTest(Test('test'))  
    # suite.addTest(Test('test分析月稅前損益'))  
    unittest.TextTestRunner().run(suite)

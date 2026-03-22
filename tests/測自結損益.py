from unittest.mock import patch
from pathlib import Path
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test載入自結損益表(self):
        from 股票分析.自結損益 import 載入自結損益表, cache
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 顯示
        from zhongwen.時 import 本月
        # 刪除指定名稱快取(cache, '載入自結損益表')
        df = 載入自結損益表('3682') # 已下市之亞太電，資料只到2020年12月
        self.assertTrue(df.empty)

    def test預測前年至次年周期數據(self):
        from 股票分析.自結損益 import 預測前年至次年周期數據
        from 股票分析.營收分析 import 預測前年至次年每股盈餘, 依台積電資本支出預測營收
        from zhongwen.表 import 顯示
        預測結果 = 依台積電資本支出預測營收('漢唐')
        顯示(預測結果)
        預測結果 = 預測前年至次年營收('台積電')

        預測結果 = 預測前年至次年每股盈餘('一零四')
        顯示(預測結果)
        # 預測結果 = 預測股利('豐興')
        # print(預測結果)

        預測結果 = 預測前年至次年周期數據(讀取測例('崑鼎'), '本月合併營業損益', '自結損益月份')
        for c in ['前年至次年各期數據', '預測說明', '趨勢起日', '數據頻率'
                 ,'趨勢方向','趨勢斜率','趨勢說明']:
            self.assertIn(c, 預測結果.index)
        _, 預測說明, _, _, 趨勢方向, *_ = 預測結果
        self.assertIn(趨勢方向, ['增加', '減少'])

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

    def test預測前年至次年每股盈餘(self):
        from 股票分析.自結損益 import 預測前年至次年每股盈餘
        from zhongwen.表 import 數據不足, 顯示

        預測項目 = ['前年至次年每股盈餘','預測說明']

        r = 預測前年至次年每股盈餘('遠東銀')
        顯示(r)
        self.assertTrue(False)
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
    # suite.addTest(Test('test預測前年至次年每股盈餘'))  
    suite.addTest(Test('test分析月稅前損益'))  
    unittest.TextTestRunner().run(suite)

from unittest.mock import patch
from pathlib import Path
import functools
import unittest
import logging
from diskcache import Cache
from pathlib import Path

cache = Cache(Path.home() / 'cache' / Path(__file__).stem)


class Test(unittest.TestCase):

    def test預測配息率(self):
        from 股票分析.股利分析 import 預測配息率
        from zhongwen.表 import 顯示
        必須欄位 = ['預測配息率', '預測配息率說明']
        r = 預測配息率('FB台50')
        顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

        r = 預測配息率('遠東銀')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

        r = 預測配息率('中再保')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)
        
    def test依歷年股利預測股利(self):
        from 股票分析.股利分析 import 依歷年股利預測股利
        from zhongwen.表 import 顯示, 數據不足
        必須欄位 = ['前年至次年股利', '預測說明']
        self.assertRaises(數據不足, 依歷年股利預測股利, 'FB台50')

        前年至次年股利, 預測說明, *_ = r = 依歷年股利預測股利('中再保')
        # 顯示(預測說明)
        for c in 必須欄位:
            self.assertIn(c, r.index)

    def test依損益表預測每股盈餘(self):
        from twse_crawler.股利分析 import 依損益表預測每股盈餘, 預測股利
        from zhongwen.表 import 顯示
        必須欄位 = ['前年至次年每股盈餘', '預測說明']

        r = 預測股利('泰銘')
        顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

    def test預測股利(self):
        from 股票分析.股利分析 import 預測股利, cache
        from 股票分析.自結損益 import 預測前年至次年每股盈餘 as 依自結損益預測每股盈餘 
        from zhongwen.表 import 數據不足, 顯示
        必須欄位 = ['前年至次年股利', '預測股利說明', '除息交易日', '除權交易日', '現金股利發放日']

        r = 預測股利('華碩')
        顯示(r)
        self.assertFalse(True)
        # for c in 必須欄位:
            # self.assertIn(c, r.index)


        r = 依自結損益預測每股盈餘('中信金')
        # 顯示(r)
        self.assertRegex(r.預測說明, '.*稅前損益.*')

        r = 預測股利('中信金')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測股利說明, '.*稅前損益.*')

        r = 預測股利('雄順')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

        r = 預測股利('創控')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

        r = 預測股利('宏洲')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)
        # self.assertRaises(數據不足, 預測股利, '宏洲')

        r = 預測股利('中再保')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

        r = 預測股利('崑鼎')
        # 顯示(r)
        for c in 必須欄位:
            self.assertIn(c, r.index)

    def test取歷年每股盈餘表(self):
        from 股票分析.股利分析 import 取歷年每股盈餘表
        from zhongwen.表 import 顯示
        r = 取歷年每股盈餘表('中再保')
        顯示(r)

    def test取股利表(self):
        from 股票分析.股票基本資料分析 import 查股票代號
        from 股票分析.股利分析 import 取股利表, cache
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 顯示
        # 刪除指定名稱快取(cache, '取股利表')
        必要欄位 = ['除權交易日', '現金股利發放日']
        df = 取股利表()
        for c in 必要欄位:
            self.assertIn(c, 必要欄位)

        s = 取股利表('富林-KY')
        顯示(s)
        return

        # 應過濾股利為零或空值者
        股利為零或空值者 = df.query(
                              '(配息==0 or 配息.isna()) '
                              'and (配股==0 or 配股.isna()) '
                              'and (特別股配息==0 or 特別股配息.isna())')
        顯示(股利為零或空值者)
        self.assertTrue(股利為零或空值者.empty)
        
        # 台泥應過濾105年重覆公告股利
        台泥股利 = df.query('公司代號=="1101"')
        # 顯示(台泥股利)
        df1101_105 = df.query('公司代號=="1101" and 股利所屬年度.dt.year==2016')
        self.assertEqual(df1101_105.shape[0], 1)

        # 鈊象112年度股利期間誤植為112年後半年度，於程式補正
        df3293 = df.query('公司代號=="3293"')
        # 顯示(df3293)
        self.assertTrue(all(df3293.股利所屬期間.map(lambda p: 'Y' in p.freqstr)))

        # 台積電目前每季配發股利，應過濾非季度股利紀錄
        df2330 = df.query('公司代號=="2330"')
        df2330['freq'] = df2330.股利所屬期間.map(lambda p: p.freqstr)
        last_freq = df2330.iloc[-1].freq
        df2330 = df2330.query('freq==@last_freq')
        self.assertTrue(all(df2330.股利所屬期間.map(lambda p: 'Q-DEC' in p.freqstr)))

        # 數字目前每半年配發股利，應過濾非半年度股利紀錄
        df5287 = df.query('公司代號=="5287"')
        df5287['freq'] = df5287.股利所屬期間.map(lambda p: p.freqstr)
        last_freq = df5287.iloc[-1].freq
        df5287 = df5287.query('freq==@last_freq')
        # 顯示(df5287)
        self.assertTrue(all(df5287.股利所屬期間.map(lambda p: 'M' in p.freqstr)))

    def test取歷年股利表(self):
        from 股票分析.股票基本資料分析 import 查股票代號
        from 股票分析.股利分析 import 取歷年股利表, cache
        from zhongwen.表 import 顯示
        必要欄位 = ['現金股利發放日']
        h1 = 取歷年股利表('富林-KY')
        顯示(h1)
        for c in 必要欄位:
            self.assertIn(c, h1.columns)

        h = 取歷年股利表()
        顯示(h)

    def test取除權息概述(self):
        from 股票分析.股票基本資料分析 import 查股票代號
        from 股票分析.股利分析 import 取股利表, 取除權息概述
        from zhongwen.表 import 顯示
        df = 取股利表()
        富林二千廿四年股利 = df.query(
            '公司代號==@查股票代號("富林-KY") and 股利所屬年度.dt.year==2024').iloc[-1]
        r = 取除權息概述(富林二千廿四年股利)
        m = '已除息5.20元'
        self.assertEqual(r, m)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test依損益表預測每股盈餘'))
    unittest.TextTestRunner().run(suite)

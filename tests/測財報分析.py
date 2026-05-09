from unittest.mock import patch
from pathlib import Path
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test取歷年營收占台積電資本資出比例(self):
        from twse_crawler.財報分析 import 取歷年營收占台積電資本資出比例
        from zhongwen.時 import 取民國年度
        from zhongwen.表 import 顯示
        df = 取歷年營收占台積電資本資出比例('漢唐')
        起年 = 取民國年度(df.財報日期.min())
        迄年 = 取民國年度(df.財報日期.max())
        最低比例 = df.營收占台積電資本資出比例.min()
        最高比例 = df.營收占台積電資本資出比例.max()
        說明 =  f"{起年.replace('年度', '')}至{迄年}"
        說明 += f"營收占台積電資本支出{最低比例:.0%}至{最高比例:.0%}間"
        print(說明)
        顯示(df)

    def test取財報彙總表(self):
        from twse_crawler.財報分析 import 取財報彙總表, 取移動年度財報彙總表
        from twse_crawler.財報分析 import cache, 取近年財報彙總表
        from 股票分析.股票基本資料分析 import 查股票代號
        from zhongwen.時 import 取日期
        from zhongwen.表 import 顯示
        cache.clear()
        df = 取移動年度財報彙總表('台積電')
        self.assertIn('自由現金流', df.columns)
        顯示(df)

    def test分析科目(self):
        from twse_crawler.財報分析 import 分析合約負債, 分析營業現金流對淨利比
        from twse_crawler.財報分析 import 杜邦分析
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.財報分析 import cache
        # from 股票分析.資產負債表分析 import cache
        from zhongwen.表 import 顯示
        cache.clear()
        # r = 分析營業現金流對淨利比('漢唐')
        s = '鈊象'
        # r = 取財報彙總表(s)
        r = 杜邦分析(s)
        顯示(r)

    def test分析存貨周轉天數(self):
        from twse_crawler.財報分析 import 分析存貨周轉天數

        r = 分析存貨周轉天數('華碩')
        評語 = '114年第2季存貨轉為年減3%，比重仍達26%，且周轉天數年減24%，連減2季，產能不足銷貨增長需求' 
        self.assertEqual(評語, r.評語)
        self.assertEqual(r.分數, 308)
        self.assertFalse(True)

        r = 分析存貨周轉天數('中化控股')
        評語 = '存貨比重' 

    def test分析現金轉換周期(self):
        from twse_crawler.財報分析 import 分析現金轉換周期, cache
        from zhongwen.表 import 顯示, 數據不足
        cache.clear()
        r = 分析現金轉換周期('鈊象')
        顯示(r)
        self.assertTrue(False)

    def test杜邦分析(self):
        from twse_crawler.財報分析 import 杜邦分析, 分析流動資產, cache, 取移動年度財報彙總表
        from twse_crawler.股票評級 import 評級股票
        from twse_crawler.財報分析 import 取近年財報彙總表
        import zhongwen.快取
        from zhongwen.表 import 表示
        # 更新財報與自結損益及營收暨股利和行情()
        # cache.clear()
        # zhongwen.快取.停止快取=True
        # r = 杜邦分析('一零四')
        df = 取近年財報彙總表('華南金')
        表示(df)
        self.assertFalse(True)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # 建構財報測例()
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test杜邦分析'))
    unittest.TextTestRunner().run(suite)

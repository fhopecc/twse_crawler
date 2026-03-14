import unittest

class Test(unittest.TestCase):

    def test抓轉交換債發行資料(self):
        from 股票分析.櫃買中心爬蟲 import 抓轉交換債發行資料, cache
        from zhongwen.表 import 顯示
        from zhongwen.快取 import 刪除指定名稱快取
        刪除指定名稱快取(cache, '抓轉交換債發行資料')
        df = 抓轉交換債發行資料()
        顯示(df)

    def test抓取上櫃股票行情(self):
        from twse_crawler.櫃買中心爬蟲 import 抓取上櫃股票行情, cache
        from zhongwen.表 import 顯示
        from zhongwen.時 import 今日
        from zhongwen.快取 import 刪除指定名稱快取
        刪除指定名稱快取(cache, '抓取上櫃股票行情')
        df = 抓取上櫃股票行情(今日)
        必須欄位 = ['代號', '名稱', '收盤']
        # 顯示(df[必須欄位], 顯示筆數=2500)
        self.assertIn('代號', df.columns)
        self.assertIn('力泰', df.名稱.values)

    def test上櫃股票行情庫(self):
        from 股票分析.櫃買中心爬蟲 import 上櫃股票行情庫
        from zhongwen.庫 import 載入上批資料
        from zhongwen.表 import 顯示
        from zhongwen.時 import 今日
        df = 載入上批資料(上櫃股票行情庫, '上櫃股票行情表', '交易日期', '交易日期')
        必須欄位 = ['代號', '名稱', '收盤', '交易日期']
        df = df.query('名稱=="力泰"')
        self.assertEqual(df.shape[0], 1) # 上批資料應該只有一批
        self.assertLess((今日-df.iloc[0].交易日期).days, 2)
        self.assertIn('力泰', df.名稱.values)
        # 顯示(df[必須欄位])
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test抓上櫃股票基本資料'))
    # suite.addTest(Test('test抓轉交換債發行資料'))
    suite.addTest(Test('test抓取上櫃股票行情'))
    unittest.TextTestRunner().run(suite)

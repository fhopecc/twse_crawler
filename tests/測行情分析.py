import unittest

class Test(unittest.TestCase):

    def test(self):
        from twse_crawler.行情分析 import 取最近上市櫃收盤行情, cache
        from twse_crawler.行情分析 import 預測報酬率
        from twse_crawler.證交所爬蟲 import 抓取上市每日收盤行情
        from twse_crawler.櫃買中心爬蟲 import 抓取上櫃股票行情
        from zhongwen.表 import 表示, 數據不足
        # cache.clear()

        r = 預測報酬率('中菲', 重新分析=True)
        必須欄位 = set(['報酬率', '預測報酬率說明', '交易日期', '收盤價'
                       ,'前年至次年股利','除息交易日', '除權交易日', '現金股利發放日'
                       ])
        self.assertTrue(必須欄位.issubset(r.index)) 
        表示(r)
        self.assertFalse(True)
        # df = 抓取上櫃股票行情('115.7.10')
        # 表示(df)
        df1 = 取最近上市櫃收盤行情('一零四')
        表示(df1)
        self.assertFalse(df1.empty)
        self.assertRaises(數據不足, 取最近上市櫃收盤行情, '二信股票')


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

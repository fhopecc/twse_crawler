import unittest

class Test(unittest.TestCase):

    def test(self):
        from twse_crawler.行情分析 import 取最近上市櫃收盤行情, cache
        from twse_crawler.行情分析 import 預測報酬率
        from zhongwen.表 import 表示, 數據不足

        必須欄位 = set(['報酬率', '預測報酬率說明', '交易日期', '收盤價'
                       ,'前年至次年股利','除息交易日', '除權交易日', '現金股利發放日'
                       ])
        r = 預測報酬率('中菲', 重新分析=True)
        self.assertTrue(必須欄位.issubset(r.index)) 
        表示(r)
        self.assertFalse(True)

        self.assertRaises(數據不足, 取最近上市櫃收盤行情, '二信股票')

        df1 = 取最近上市櫃收盤行情('中菲')
        表示(df1)
        self.assertFalse(df1.empty)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

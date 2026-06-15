from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test(self):
        from twse_crawler.現流表分析 import 取移動年度現流表, cache
        from twse_crawler.現流表分析 import 取現流表, 取累積現流表
        from twse_crawler.現流表分析 import 分析現金流
        from zhongwen.表 import 表示
        r = 分析現金流('鈊象')
        表示(r)
        self.assertFalse(True)
        cache.clear()

        df = 取現流表()
        必須欄位 = set(['股票代號', '財報日期'])
        self.assertTrue(必須欄位.issubset(set(df.columns)))

        df = 取移動年度現流表()
        必須欄位 = set(['股票代號', '財報日期'])
        self.assertTrue(必須欄位.issubset(set(df.columns)))


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

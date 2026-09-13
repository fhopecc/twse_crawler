from unittest.mock import patch
from pathlib import Path
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.匯率分析 import 抓取年度美元兌新台幣匯率
        from twse_crawler.匯率分析 import 取美元匯率, cache
        from twse_crawler.匯率分析 import 預測次年底美元匯率
        from twse_crawler.匯率分析 import 以匯率預測次年底業外損益
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 表示
        cache.clear()
        股票 = '泰銘'
        df = 以匯率預測次年底業外損益(股票, 回滾季數=4)
        表示(df, 顯示索引=True)
        self.assertFalse(True)
        # 表示(df.預估各季值, 顯示索引=True)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

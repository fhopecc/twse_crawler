from unittest.mock import patch
from pathlib import Path
import unittest
import logging

logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test(self):
        from twse_crawler.損益表分析 import 取累積損益表, 取損益表, 取移動年度損益表
        from twse_crawler.損益表分析 import 預估業外損益
        from twse_crawler.股票基本資料分析 import 查股票代號, cache as cacheb
        from twse_crawler.損益表分析 import 檢測營收及毛利率關係
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 表示
        import twse_crawler
        twse_crawler.損益表分析.cache.clear()
        r = 檢測營收及毛利率關係('泰銘')
        表示(r)
        self.assertFalse(True)
        r = 預估業外損益('數字')
        表示(r, 顯示索引=True)
        必須欄位 = set(['股票代號', '財報類型', '財報日期'])
        df = 取損益表()
        self.assertTrue(必須欄位.issubset(set(df.columns)))

        df = 取移動年度損益表()
        self.assertTrue(必須欄位.issubset(set(df.columns)))

        
if __name__ == '__main__':
    from twse_crawler.損益表分析 import cache
    from zhongwen.快取 import 刪除指定名稱快取
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    # 刪除指定名稱快取(cache, '取損益表')
    unittest.main()

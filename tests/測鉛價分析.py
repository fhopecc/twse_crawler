from unittest.mock import patch
import unittest
class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.鉛價分析 import 取鉛價, 預測次年底鉛價
        from twse_crawler.鉛價分析 import 以鉛價預測次年每股盈餘
        from twse_crawler.鉛價分析 import 以鉛價預測次年底毛利率, cache
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.鉛價分析 import 預測次年底營收
        from zhongwen.表 import 表示
        from zhongwen.時 import 今年數
        from zhongwen.快取 import 刪除指定名稱快取
        # 刪除指定名稱快取(cache, '以鉛價預測次年每股盈餘')
        股票 = '泰銘'
        df = 以鉛價預測次年每股盈餘(股票)
        print(df.預測說明)
        self.assertTrue(False)
        h = 取財報彙總表(股票)
        m = 以鉛價預測次年底毛利率(股票)
        # r = 以輔助季數據預測至次年底各季數據(股票,輔助數據預測值=m.預測前年至次年底各季毛利率)
        表示(r.每季預測值)

if __name__ == '__main__':
    import logging
    # logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger("prophet").setLevel(logging.CRITICAL)
    logging.getLogger("cmdstanpy").setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

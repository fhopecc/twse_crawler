from unittest.mock import patch
import unittest
class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.鉛價分析 import 取鉛價, 預測次年底鉛價
        from twse_crawler.鉛價分析 import 以鉛價預測次年底毛利率
        from twse_crawler.鉛價分析 import 以鉛價預測次年淨利
        from twse_crawler.匯率分析 import 以匯率預測次年底業外損益
        from zhongwen.表 import 表示
        import pandas as pd
        股票 = '泰銘'
        df = 以鉛價預測次年淨利(股票)
        表示(df.tail(100), 顯示索引=True)
        self.assertTrue(False)
        # 改到 預測鉛價要加上回滾日數
        print(df)
        print(df.預估方法說明)
        print(df.預估說明)
        h = 取財報彙總表(股票)
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

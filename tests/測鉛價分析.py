from unittest.mock import patch
import unittest
class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.鉛價分析 import 以大宗商品價預測至次年底各季財務數據, 取鉛價
        from twse_crawler.鉛價分析 import 以鉛價預測前年至次年每股盈餘
        from twse_crawler.鉛價分析 import 預測至次年度商品價, 抓取年度鉛價
        from zhongwen.表 import 表示
        import pandas as pd
        r = 預測至次年度商品價(取鉛價())         
        表示(r[1])
        self.assertFalse(True) 
        r = 以鉛價預測前年至次年每股盈餘('泰銘')
        r = 抓取年度鉛價()
        r = 預測至次年度商品價(取鉛價())
        表示(r[1])

if __name__ == '__main__':
    import logging
    # logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger("prophet").setLevel(logging.CRITICAL)
    logging.getLogger("cmdstanpy").setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 預測次年底營收, 取歷月營收表
        from twse_crawler.月營收逐步推估淨利 import 自月營收逐步推估淨利
        from twse_crawler.月營收逐步推估淨利 import 以月營收逐步推估淨利
        from zhongwen.表 import 表示
        股票 = '遠傳'
        r = 以月營收逐步推估淨利(股票)
        表示(r, 顯示索引=True)
        self.assertFalse(True)
        h1 = 取歷月營收表(股票)
        h2 = 取財報彙總表(股票)
        # 表示(h[['營收', '營利', '業外損益', '稅前淨利', '淨利']], 顯示索引=True)
        r = 自月營收逐步推估淨利(h1.營收, h2.營利, h2.業外損益, h2.稅前淨利, h2.淨利)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

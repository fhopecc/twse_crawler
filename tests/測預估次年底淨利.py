from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.預估次年底淨利 import 預估次年底淨利
        from twse_crawler.預估次年底淨利 import 以淨利預測次年每股盈餘
        from twse_crawler.損益表分析 import 取損益表
        from zhongwen.表 import 表示
        股票 = '星宇航空'
        import zhongwen.快取
        zhongwen.快取.停止快取=True
        r = 預估次年底淨利(股票)
        表示(r)
        self.assertFalse(True)
        r = 以淨利預測次年每股盈餘(股票)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

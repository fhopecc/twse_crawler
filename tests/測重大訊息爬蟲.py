from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.重大訊息爬蟲 import 抓取重大訊息, 抓取重大訊息詳細資料
        from zhongwen.表 import 顯示
        from zhongwen.時 import 取日期
        df = 抓取重大訊息("113.12.15")
        p = df.iloc[0].詳細資料
        j = 抓取重大訊息詳細資料(p)
        # 顯示(df)
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test'))  # 指定測試
    unittest.TextTestRunner().run(suite)

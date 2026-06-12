import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.蒐整財務資訊 import 取股票資料最近時間
        from twse_crawler.蒐整財務資訊 import 蒐整財務資訊
        from twse_crawler.營收分析 import 以營收預測次年每股盈餘
        from twse_crawler.營收分析 import 取歷月營收表
        from zhongwen.程式 import 列出函數執行時間表
        import twse_crawler.營收分析
        from zhongwen.表 import 表示
        蒐整財務資訊()
        self.assertFalse(True)

        股票 = '東生華'
        twse_crawler.營收分析.cache.clear()
        s = 取股票資料最近時間(股票) 
        表示(s) 
        r = 以營收預測次年每股盈餘(股票)
        表示(r) 
        列出函數執行時間表()
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.營收預測營利模型 import 以營收預測次年營利
        from twse_crawler.營收預測營利模型 import 以營收預測次年淨利
        from twse_crawler.營收預測營利模型 import 表達預估方法乙
        from twse_crawler.營收預測營利模型 import 表達預估說明乙
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 預測次年底營收
        from zhongwen.表 import 表示
        股票 = '泰銘'
        m = 以營收預測次年淨利(股票)
        表示(m)
        # 表示(m.預估各季值)
        # 表示(m.預測營收結果.預估各月值.tail(36))
        # 表示(m.預測營收結果.預估每季總值)
        # r = 表達預估方法(m)
        # r = 表達預估說明(m)
        # print(r)
        self.assertFalse(True)

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

import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.無腦預測至次年底每季值 import 無腦預估至次年底每季值
        from twse_crawler.無腦預測至次年底每季值 import 取最佳無腦預估至次年底每季值模型
        from twse_crawler.預估次年底淨利 import 預估次年底淨利
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 預測次年底營收
        from zhongwen.表 import 表示
        股票 = '數字'
        h = 取財報彙總表(股票)
        r = 預估次年底淨利(股票)
        表示(r)
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

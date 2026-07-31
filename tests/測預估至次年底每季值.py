import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.預估至次年底每季值 import 取預估至次年底每季值模型
        from twse_crawler.財報分析 import 取財報彙總表
        from zhongwen.表 import 表示
        股票 = '一零四'
        h = 取財報彙總表(股票)
        r = 取預估至次年底每季值模型(h.淨利)
        表示(r)
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

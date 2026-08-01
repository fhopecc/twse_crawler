import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.以單元迴歸預估至次年底每季值 import 以單元迴歸預估至次年底每季值
        from twse_crawler.預估至次年底每季值 import 預估至次年底每季值丙式
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 預測次年底營收
        from zhongwen.表 import 表示
        股票 = '是方'
        預估營收結果 = 預測次年底營收(股票)
        預測營收 = 預估營收結果.預估每季總值
        df = 取財報彙總表(股票)
        r = 以單元迴歸預估至次年底每季值(df.營收, df.營利, 預測營收)
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

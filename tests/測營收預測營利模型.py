import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.營收預測營利模型 import 取季營收預測營利模型
        from twse_crawler.營收預測營利模型 import 以營收預測次年度營利
        from twse_crawler.營收預測營利模型 import 以營收預測次年每股盈餘
        from twse_crawler.營收預測營利模型 import 表達預估方法乙
        from twse_crawler.營收預測營利模型 import 表達預估說明乙
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 預測次年底營收
        from zhongwen.表 import 表示
        股票 = '中宇'
        # 最近營收月份 = 預估營收結果.最近歷史值時間
        # 表示(預測營收, 顯示索引=True)
        # future_index = 預測營收.index[預測營收.index > 歷季損益表.index.max()]
        # m = 取季營收預測營利模型(df)
        預估營收結果 = 預測次年底營收(股票)
        預測營收 = 預估營收結果.預估每季總值.預估每季營收
        df = 取財報彙總表(股票)
        # m = 以營收預測次年度營利(預測營收, df)
        m = 以營收預測次年每股盈餘(股票)
        表示(m)
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

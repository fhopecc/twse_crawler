from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.預估次年底 import 預估次年底價格, 依外部季數據預估次年底數值
        from twse_crawler.預估次年底 import 預估至次年底每月值
        from twse_crawler.預估次年底 import 預估至次年底每季值
        from zhongwen.表 import 表示
        from twse_crawler.鉛價分析 import 取鉛價
        from twse_crawler.財報分析 import 取財報彙總表
        from twse_crawler.營收分析 import 取歷月營收表
        import pandas as pd
        歷季損益表 = 取財報彙總表('泰銘')
        歷季損益表 = 歷季損益表.set_index(歷季損益表.財報日期.dt.to_period('Q'))
        r = 預估至次年底每季值(歷季損益表.業外損益)
        表示(r.預估每季值, 顯示索引=True) 
        self.assertFalse(True)

        r = 取歷月營收表('泰銘').set_index('營收月份').營收
        r = 預估至次年底每月值(r)
        表示(r.預估每季總值, 顯示索引=True) 
        print(r.模型準確度說明)
        # 表示(r.預估季值, 顯示索引=True)
        print(r.模型準確度說明)
        表示(r.預估價, 顯示索引=True) 
        表示(r.預估季價, 顯示索引=True) 
 
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

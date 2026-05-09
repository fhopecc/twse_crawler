from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from 股票分析.產險分析 import 取產險業務概況表
        from zhongwen.表 import 顯示
        df = 取產險業務概況表()
        顯示(df)

    def test分析產險公司(self):
        from 股票分析.產險分析 import 分析產險公司
        from zhongwen.表 import 顯示
        r = 分析產險公司('新產')
        顯示(r)


 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test'))
    suite.addTest(Test('test分析產險公司'))
    unittest.TextTestRunner().run(suite)

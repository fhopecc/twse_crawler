from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test評級股票(self):
        from twse_crawler.股票評級 import 評級股票, 分析成長性, 分析經營效率
        from twse_crawler.股票評級 import 取淨利成長領先指標
        from twse_crawler.股票評級 import 取營利成長領先指標, 取業外成長領先指標
        from twse_crawler.股票評級 import cache as acache
        from twse_crawler.自結損益 import cache as bcache
        from 股票分析.人工分析 import cache as dcache
        from zhongwen.表 import 表示
        import zhongwen.快取
        bcache.clear()
        zhongwen.快取.停止快取=True
        r = 評級股票('6890', 告示例外=True)
        表示(r)
        self.assertFalse(True)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()
    # suite = unittest.TestSuite()
    # suite.addTest(Test('test評級股票')) 
    # suite.addTest(Test('test'))
    # unittest.TextTestRunner().run(suite)

from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from 股票分析.淨利分析 import 分析淨利率
        from 股票分析.財報分析 import cache
        from zhongwen.表 import 顯示
        import zhongwen.快取 
        # cache.clear()
        # zhongwen.快取.停止快取=True
        r = 分析淨利率('東生華')
        顯示(r)
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test'))  # 指定測試
    unittest.TextTestRunner().run(suite)

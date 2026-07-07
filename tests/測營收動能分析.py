import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.營收動能分析 import 分析營收動能, 分析崑鼎型公司
        import twse_crawler.財報分析
        import twse_crawler.資產負債表分析
        import twse_crawler.現流表分析
        from zhongwen.表 import 表示
        # twse_crawler.現流表分析.cache.clear()
        # twse_crawler.資產負債表分析.cache.clear()
        # twse_crawler.財報分析.cache.clear()
        s = 分析崑鼎型公司('遠傳')
        print(s)
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

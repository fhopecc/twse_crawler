from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from zhongwen.表 import 表示
        from twse_crawler.現金週轉天數分析 import 分析現金週轉天數
        from twse_crawler.現金週轉天數分析 import 評定現金週轉天數分數
        import twse_crawler.資產負債表分析
        import twse_crawler.財報分析
        twse_crawler.資產負債表分析.cache.clear()
        twse_crawler.財報分析.cache.clear()
        r = 評定現金週轉天數分數('鈊象')
        表示(r)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

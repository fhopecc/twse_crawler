from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from zhongwen.表 import 表示
        from twse_crawler.營利率分析 import 分析營利率
        r = 分析營利率('智基')
        表示(r)
        self.assertFalse(True)
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.權益報酬率分析 import 分析股東權益報酬率        
        from zhongwen.表 import 表示
        r = 分析股東權益報酬率('鈊象')
        表示(r)
 
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

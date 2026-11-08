import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test(self):
        from twse_crawler.權益報酬率分析 import 分析股東權益報酬率, 評定權益報酬率分數
        from zhongwen.表 import 表示
        股票 = '元大期'
        r = 評定權益報酬率分數(股票)
        表示(r)
        self.assertFalse(True)
if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

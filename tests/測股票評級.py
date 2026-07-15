from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test評級股票(self):
        from twse_crawler.股票評級 import 評級股票, 分析成長性, 分析經營效率
        from twse_crawler.股票評級 import 取淨利成長領先指標
        from twse_crawler.股票評級 import 取營利成長領先指標, 取業外成長領先指標
        from twse_crawler.股票評級 import 取在線分析結果明細, 評定基礎分數
        from zhongwen.表 import 表示
        import zhongwen.快取
        r = 評級股票('亞泥', 告示例外=True)
        # r = 評定基礎分數('崑鼎')
        表示(r)
        self.assertFalse(True)
        df = 取在線分析結果明細('一零四')
        表示(df)
        zhongwen.快取.停止快取=True

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    unittest.main()

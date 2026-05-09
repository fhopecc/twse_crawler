from unittest.mock import patch
from pathlib import Path
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def test(self):
        from twse_crawler.資產負債表分析 import 分析骯髒科目, 分析負債
        from twse_crawler.資產負債表分析 import 分析客戶保證金
        from zhongwen.表 import 顯示
        df = 分析負債('鈊象')
        顯示(df)

        self.assertTrue(False)

        df = 分析骯髒科目('台灣高鐵')
        顯示(df)

        df = 分析客戶保證金("台泥")
        顯示(df)
        self.assertTrue(df.分數==0)

        df = 分析客戶保證金("元大期")
        顯示(df)

if __name__ == '__main__':
    import pandas as pd
    import logging
    import warnings
    pd.options.mode.chained_assignment = None 
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test'))
    unittest.TextTestRunner().run(suite)

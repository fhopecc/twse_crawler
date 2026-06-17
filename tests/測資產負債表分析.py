from unittest.mock import patch
from pathlib import Path
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def test(self):
        from twse_crawler.資產負債表分析 import 分析骯髒科目, 分析負債
        from twse_crawler.資產負債表分析 import 分析客戶保證金
        from twse_crawler.資產負債表分析 import 取資產負債表
        from twse_crawler.負債分析 import 分析有息負債
        import twse_crawler
        from zhongwen.表 import 顯示, 表示
        r = 分析有息負債('一零四')
        表示(r)
        self.assertFalse(True)
        twse_crawler.資產負債表分析.cache.clear()
        df = 取資產負債表()
        cols = ['股票代號'
               ,'母公司暨子公司所持有之母公司庫藏股股數（單位：股）'
               ,'合約負債－流動', "客戶保證金專戶"
               ,'應付帳款', '本期所得稅負債', '其他應付款' 
               ,"短期借款", "應付短期票券" 
               ,'負債準備－流動', '租賃負債－流動', '其他流動負債' 
               ,'非流動負債合計'
               ,"一年或一營業週期內到期長期負債" 
               ,"長期借款", "應付公司債" 
               ]
        for c in cols:
            self.assertIn(c, df.columns)
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

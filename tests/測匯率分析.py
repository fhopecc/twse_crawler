from unittest.mock import patch
from pathlib import Path
import unittest

__美元兌新台幣匯率測例檔__ = Path(__file__).parent / '美元兌新台幣匯率測例.pkl'

def 建構美元兌新台幣匯率測例():
    '114年6月13日建構'
    from 股票分析.資產負債表分析 import 下載美元兌新台幣十年匯率, cache
    cache.clear()
    df = 下載美元兌新台幣十年匯率()
    df.to_pickle(__美元兌新台幣匯率測例檔__)

def 讀取美元兌新台幣匯率測例():
    '114年6月13日建構'
    import pandas as pd
    return pd.read_pickle(__美元兌新台幣匯率測例檔__)

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'
    def test下載美元兌新台幣十年匯率(self):
        from 股票分析.匯率分析 import 下載美元兌新台幣十年匯率, 設定執行環境
        from zhongwen.快取 import 刪除指定名稱快取
        from 股票分析.資產負債表分析 import cache
        from zhongwen.表 import 顯示
        # 設定執行環境()
        # 刪除指定名稱快取(cache, '下載美元兌新台幣十年匯率')
        df = 下載美元兌新台幣十年匯率()
        顯示(df.tail(10), 顯示索引=True)

    @patch('股票分析.匯率分析.下載美元兌新台幣十年匯率')
    def test即時分析美元兌新台幣匯率(self, 下載美元兌新台幣十年匯率):
        from 股票分析.匯率分析 import 即時分析美元兌新台幣匯率
        from zhongwen.時 import 取日期
        from zhongwen.表 import 顯示
        import zhongwen.時
        import numpy as np
        zhongwen.時.今日 = 取日期('1140613')
        下載美元兌新台幣十年匯率.return_value = 讀取美元兌新台幣匯率測例()
        r = 即時分析美元兌新台幣匯率()
        self.assertEqual(r['交易日'], 取日期('1140613'))
        self.assertEqual(np.floor(r['匯率']*100)/100, 29.55)
        匯率說明 = '今日美元兌新台幣29.56元，較上季末減少10.93%'
        self.assertEqual(r.說明, 匯率說明)

    def test分析匯率(self):
        from 股票分析.匯率分析 import 分析匯率, 設定執行環境
        from zhongwen.表 import 顯示
        # 設定執行環境()
        # r = 分析匯率('泰銘')
        r = 分析匯率('中再保')
        顯示(r)
        self.assertFalse(True)

    def test取國外投資比例(self):
        from 股票分析.匯率分析 import 取國外投資比例
        from 股票分析.人工分析 import 取本人股票筆記
        from zhongwen.表 import 顯示
        n = 取本人股票筆記('中再保').iloc[-1].筆記
        print(n)
        r = 取國外投資比例(n)
        顯示(r)


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test下載美元兌新台幣十年匯率'))
    suite.addTest(Test('test分析匯率'))
    # suite.addTest(Test('test取國外投資比例'))
    unittest.TextTestRunner().run(suite)

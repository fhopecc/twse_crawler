from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test分析同比差異主次因(self):
        from 股票分析.財報分析 import 杜邦分析, cache, 取財報彙總表
        from 股票分析.趨勢分析 import 分析同比差異主次因
        from 股票分析.損益表分析 import 取損益表, cache
        from zhongwen.表 import 顯示
        # cache.clear()
        股票 = '中再保'
        歷季財報 = 取財報彙總表(股票)
        淨利 = 歷季財報.淨利.rolling(window=4).sum()
        營收 = 歷季財報.營收.rolling(window=4).sum()
        資產 = 歷季財報.資產總計
        歷季財報['權益總計'] = 歷季財報.權益總計.fillna(歷季財報.權益)
        股東權益 = 歷季財報.權益總計
        歷季財報['股東權益報酬率'] = 股東權益報酬率 = 淨利 / 股東權益
        歷季財報['淨利率'] = 淨利率 = 淨利 / 營收
        歷季財報['資產周轉率'] = 資產周轉率 = 營收 / 資產
        歷季財報['權益乘數'] = 權益乘數 = 資產 / 股東權益
        r = 分析同比差異主次因(歷季財報, 股票, '股東權益報酬率'
                              ,['淨利率', '資產周轉率', '權益乘數']
                              ,子項結合方式='積'
                              )
        顯示(r, 顯示索引=True)
        self.assertTrue(False)

    def test分析本季同比(self):
        from 股票分析.淨利分析 import 分析淨利
        r = 分析淨利('泰銘')
        print(r.說明)

    def test分析歷月數據增減情形(self):
        from 股票分析.趨勢分析 import 分析歷月數據增減情形
        from 股票分析.自結損益 import 取自結損益表
        from zhongwen.表 import 顯示
        h = 取自結損益表('遠東銀')
        r = 分析歷月數據增減情形(h, '本月合併稅前損益', '自結損益月份')
        顯示(r)
        self.assertTrue(False)

    def test分析歷季數據增減情形(self):
        from 股票分析.財報分析 import 分析財報
        from zhongwen import 快取
        快取.停止快取=True
        testees = [#'志超'
                 '三圓'
                 ]
        for s in testees:
            print(f'分析{s}')
            r = 分析財報(s)
            print(r.評語)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test分析同比差異主次因'))
    unittest.TextTestRunner().run(suite)

from pathlib import Path
import tempfile
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def test預測營收(self):
        from twse_crawler.營收分析 import 預測次年底營收
        from twse_crawler.營收分析 import 以營收預測次年每股盈餘
        from twse_crawler.鉛價分析 import 以鉛價預測次年每股盈餘
        from twse_crawler.資產負債表分析 import 取資產負債表
        from zhongwen.表 import 表示
        import matplotlib.pyplot as plt
        公司 = '中宇'
        df = 取資產負債表(公司)
        # 表示(df['合約負債－流動'], 顯示筆數=2000, 顯示索引=True) 
        df['合約負債－流動'].plot()
        plt.show()
        self.assertFalse(True)
        r = 預測次年底營收(公司)
        表示(r.預估每月值, 顯示筆數=2000, 顯示索引=True) 
        r.預估每月值.plot()
        # 表示(r.預估每月值, 顯示索引=True, 顯示筆數=1000)
        # print(r.預估每月值)

    def test依台積電月營收預測次年資本支出(self):
        from twse_crawler.營收分析 import 依台積電月營收預測次年資本支出
        from zhongwen.表 import 顯示
        必要欄位 = ['截至次年各季台積電資本支出'] 
        預測結果 = 依台積電月營收預測次年資本支出()
        顯示(預測結果)
        self.assertTrue(all(c in 預測結果.index for c in 必要欄位))

    def test依台積電資本支出預測營收(self):
        from twse_crawler.財報爬蟲 import 下載季報包, 爬取損益表, 爬取資產負債表, 爬取現流表
        from twse_crawler.營收分析 import 依台積電資本支出預測營收
        from twse_crawler.現流表分析 import 取現流表, cache
        from zhongwen.時 import 取期間
        from zhongwen.表 import 顯示
        預測結果 = 依台積電資本支出預測營收('漢唐')
        顯示(預測結果)

    def test分析月營收(self):
        from twse_crawler.損益表分析 import 取損益表, cache
        from twse_crawler.營收分析 import 分析月營收, 預測前年至次年每股盈餘
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 表示
        import zhongwen.快取 

        r = 分析月營收('威盛')
        表示(r)
        self.assertFalse(True)

        zhongwen.快取.停止快取=True
        r = 預測前年至次年每股盈餘('泰銘')
        表示(r)


        s = 營收分析 = 分析月營收('志超', 重新分析=True)
        評語 = '114年6月營收轉為年增2%，102年1月轉為衰退，且111年12月以來平均按月減少794萬餘元'
        self.assertIn(評語, s.評語)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('urllib3').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test預測營收'))
    unittest.TextTestRunner().run(suite)

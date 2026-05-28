from pathlib import Path
import tempfile
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def test預測前年至次年每股盈餘(self):
        from twse_crawler.營收分析 import 以營收預測次年每股盈餘
        from zhongwen.表 import 表示
        s = 以營收預測次年每股盈餘('新產')
        print(s.預估說明)
        print(s.預估方法說明)
        self.assertFalse(True)

    def test預測前年至次年營收(self):
        from twse_crawler.營收分析 import 預測前年至次年營收
        from zhongwen.表 import 顯示, 數據不足

        self.assertRaises(數據不足, 預測前年至次年營收, '凌航')

        預測結果 = 預測前年至次年營收('台積電')
        顯示(預測結果)

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
    # suite.addTest(Test('test取歷月營收表'))
    # suite.addTest(Test('test預測前年至次年營收'))
    # suite.addTest(Test('test依台積電月營收預測次年資本支出'))
    # suite.addTest(Test('test依台積電資本支出預測營收'))
    suite.addTest(Test('test預測前年至次年每股盈餘'))
    # suite.addTest(Test('test分析月營收'))
    unittest.TextTestRunner().run(suite)

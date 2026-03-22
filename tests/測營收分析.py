from pathlib import Path
import tempfile
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def test取歷月營收表(self):
        from 股票分析.營收分析 import 取歷月營收表
        from zhongwen.時 import 今日, 上月
        from zhongwen.表 import 顯示
        鈊象歷月營收 = 取歷月營收表('鈊象')
        # 顯示(鈊象歷月營收)
        if 今日.day > 10:
            self.assertEqual(鈊象歷月營收.營收月份.iloc[-1], 上月)

    def test預測前年至次年每股盈餘(self):
        from 股票分析.營收分析 import 預測前年至次年每股盈餘
        from 股票分析.股利分析 import 預測股利
        from zhongwen.表 import 顯示, 數據不足
        預測項目 = ['前年至次年每股盈餘', '預測說明'
                   ,'趨勢起日','數據頻率','趨勢方向','趨勢斜率','趨勢說明'
                   ]
        r = 預測前年至次年每股盈餘('華碩')
        顯示(r)
        self.assertFalse(True)

        r = 預測前年至次年每股盈餘('意騰-KY')

        self.assertRaises(數據不足, 預測前年至次年每股盈餘, '凌航')

        r = 預測前年至次年每股盈餘('一零四')
        self.assertEqual(len(r), 13)


        r = 預測前年至次年每股盈餘('中化控股')

        r = 預測前年至次年每股盈餘('數字')
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營收、營利、業外損益及稅前損益')

        r = 預測前年至次年每股盈餘('基士德-KY')
        # 顯示(r)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營收、營利、業外損益及稅前損益')

        r = 預測前年至次年每股盈餘('富邦金')
        # 顯示(r)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營收及稅前損益')

        r = 預測前年至次年每股盈餘('中再保')
        # 顯示(r)
        for c in 預測項目:
            self.assertIn(c, r.index)
        self.assertRegex(r.預測說明, '營收、營利、業外損益及稅前損益')

        r = 預測前年至次年每股盈餘('生合')

    def test預測前年至次年營收(self):
        from 股票分析.營收分析 import 預測前年至次年營收
        from zhongwen.表 import 顯示, 數據不足

        self.assertRaises(數據不足, 預測前年至次年營收, '凌航')

        預測結果 = 預測前年至次年營收('台積電')
        顯示(預測結果)

    def test依台積電月營收預測次年資本支出(self):
        from 股票分析.營收分析 import 依台積電月營收預測次年資本支出
        from zhongwen.表 import 顯示
        必要欄位 = ['截至次年各季台積電資本支出'] 
        預測結果 = 依台積電月營收預測次年資本支出()
        顯示(預測結果)
        self.assertTrue(all(c in 預測結果.index for c in 必要欄位))

    def test依台積電資本支出預測營收(self):
        from 股票分析.財報爬蟲 import 下載季報包, 爬取損益表, 爬取資產負債表, 爬取現流表
        from 股票分析.營收分析 import 依台積電資本支出預測營收
        from 股票分析.現流表分析 import 取現流表, cache
        from zhongwen.時 import 取期間
        from zhongwen.表 import 顯示
        預測結果 = 依台積電資本支出預測營收('漢唐')
        顯示(預測結果)

    def test分析月營收(self):
        from 股票分析.損益表分析 import 取損益表, cache
        from twse_crawler.營收分析 import 分析月營收
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 顯示
        
        s = 營收分析 = 分析月營收('鈊象', 重新分析=True)
        顯示(s)
        self.assertFalse(True)

        r = 分析月營收('青鋼', 重新分析=True)
        # 顯示(r)
        評語 = '114年6月營收轉為年減0%，102年1月轉為成長，且111年12月以來平均按月增加13萬餘元'
        self.assertEqual(r.評語, 評語)

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
    # suite.addTest(Test('test預測前年至次年每股盈餘'))
    suite.addTest(Test('test分析月營收'))
    unittest.TextTestRunner().run(suite)

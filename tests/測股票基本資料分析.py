import unittest
import logging

class Test(unittest.TestCase):

    def test載入護城河公司清單(self):
        from 股票分析.股票基本資料分析 import 載入護城河公司清單
        from zhongwen.表 import 顯示
        df = 載入護城河公司清單()
        # 顯示(df)
        self.assertIsInstance(df.iloc[0].股票代號, str)

    def test查股票代號及簡稱(self):
        from 股票分析.股票基本資料分析 import 查股票代號, 查股票簡稱, cache
        from zhongwen.快取 import 刪除指定名稱快取
        # 刪除指定名稱快取(cache, '查股票代號')
        cache.clear()
        self.assertEqual(查股票代號('二信股票'), '二信股票')
        self.assertEqual(查股票代號('好樂迪'), '9943')
        self.assertEqual(查股票代號('台通'), '8011')
        self.assertEqual(查股票代號('台通光電'), '8011')
        self.assertEqual(查股票代號('崑鼎'), '6803')
        self.assertEqual(查股票代號('6803'), '6803')
        self.assertEqual(查股票代號('志超'), '8213')
        self.assertEqual(查股票代號('8213'), '8213')
        self.assertEqual(查股票代號('2851'), '2851')
        self.assertEqual(查股票代號('青鋼'), '8930')
        self.assertEqual(查股票代號('8930'), '8930')
        self.assertEqual(查股票代號('數字'), '5287')
        self.assertEqual(查股票代號('5287'), '5287')
        self.assertEqual(查股票代號('台泥'), '1101')
        self.assertEqual(查股票代號('統一'), '1216')
        self.assertEqual(查股票代號('鈊象'), '3293')
        self.assertEqual(查股票代號('智基'), '6294')
        self.assertEqual(查股票代號('元大期'), '6023')
        self.assertEqual(查股票代號('富邦台50'), '006208')
        self.assertEqual(查股票代號('FB台50'), '006208')
        self.assertEqual(查股票代號('京元電子'), '2449')
        self.assertEqual(查股票簡稱('006208'), 'FB台50')
        self.assertEqual(查股票簡稱('4550'), '長佳')
        self.assertEqual(查股票簡稱('2851'), '中再保')
        self.assertEqual(查股票簡稱('021021'), '永豐期貨')

        # df = df.query('公司代號=="9927"')
        # 顯示(df)

    def test取公司營收組成(self):
        from 股票分析.股票基本資料分析 import 取公司營收組成, 查股票代號, cache
        from zhongwen.快取 import 刪除指定名稱快取
        r = 取公司營收組成(查股票代號('華碩'))
        self.assertIn('筆電及桌電', r)
        r = 取公司營收組成(查股票代號('基士德-KY'))
        self.assertIn('水泵', r)
        r = 取公司營收組成(查股票代號('FB台50'))
        self.assertIn('', r)

    def test取股票基本資料分析彙總表(self):
        from twse_crawler.股票基本資料分析 import 取股票基本資料彙總表, cache 
        from zhongwen.表 import 顯示, 檢核欄位資料型態是否為字串
        from zhongwen.快取 import 刪除指定名稱快取
        cache.clear()
        df = 取股票基本資料彙總表()
        必要欄位 = ['公司代號', '供氣區域', '發行可轉債', '產業類別']
        for 欄位 in 必要欄位:
            self.assertIn(欄位, df.columns)
        self.assertTrue(檢核欄位資料型態是否為字串(df, '公司代號'))
        self.assertFalse(df.發行可轉債.isna().all())
        df = 取股票基本資料彙總表('泰銘')
        顯示(df)

    def test分析公司產業(self):
        from 股票分析.股票基本資料分析 import 分析公司產業, cache
        cache.clear()
        r = 分析公司產業('台泥')
        print(r.評語)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test取股票基本資料分析彙總表'))
    unittest.TextTestRunner().run(suite)

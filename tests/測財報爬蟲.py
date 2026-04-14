from pathlib import Path
import unittest
import logging
logger = logging.getLogger(Path(__file__).stem)

class Test(unittest.TestCase):

    def testHelper(self):
        from twse_crawler.財報爬蟲 import 財報期間字串, 財報檔
        from zhongwen.date import 季末
        self.assertEqual(財報期間字串(季末(2023, 2)), 'From20230101To20230630')

    def test_get_report_info(self):
        from twse_crawler.財報爬蟲 import 取基本資料
        from zhongwen.date import 取日期
        股票代號, 財報類型, 財報日期 = 取基本資料(self.bytes2)
        self.assertEqual(股票代號, '5287')
        self.assertEqual(財報類型, '合併')
        self.assertEqual(財報日期, 取日期('2024.6.30'))

    def test_para_report_reader(self):
        from twse_crawler.財報爬蟲 import 平行讀取財報資料, 取資產負債表
        df = 平行讀取財報資料(2019, 1, 取資產負債表)
        self.assertIn('股票代號', df.columns)

    def test_scratch_BS(self):
        from twse_crawler.財報爬蟲 import 爬取資產負債表, 資產負債資料庫
        from zhongwen.batch_data import 取資料表內容
        from zhongwen.date import 取日期
        df = 爬取資產負債表(取日期('2024.6.30'))
        self.assertIn('股票代號', df.columns)
        df = 取資料表內容(資產負債資料庫, '資產負債表')
        self.assertIn('股票代號', df.columns)
        self.assertIn('母公司暨子公司所持有之母公司庫藏股股數（單位：股）', df.columns)

    def test取資產負債表(self):
        from twse_crawler.財報爬蟲 import 取資產負債表
        from zhongwen.date import 取日期
        from zhongwen.表 import 顯示
        漢唐財報 =  Path(__file__).parent / 'tifrs-fr1-m1-ci-cr-2404-2024Q2.html'
        df = 取資產負債表(漢唐財報)
        self.assertEqual(df.loc[0, '股票代號'], '2404')
        self.assertEqual(df.loc[0, '股本合計'],  1_905_867_000)
        self.assertEqual(df.loc[0, '權益總額'], 10_908_914_000)
        self.assertEqual(df.loc[0, '母公司暨子公司所持有之母公司庫藏股股數（單位：股）']
                        ,3_000_000)

    def test取損益表(self):
        from twse_crawler.財報爬蟲 import 取損益表
        from zhongwen.date import 取日期
        from zhongwen.表 import 顯示
        
        中鋼財報 = Path(__file__).parent / 'tifrs-fr1-m1-ci-cr-2002-2025Q2.html'
        df = 取損益表(中鋼財報)
        self.assertEqual(df['營業利益（損失）'].iloc[-1], -1243478000)

        # 台積電財報 = Path(__file__).parent / 'tifrs-fr1-m1-ci-cr-2330-2025Q2.html'
        # df = 取損益表(台積電財報)
        # 顯示(df)

    def test取現流表(self):
        from twse_crawler.財報爬蟲 import 取現流表
        from zhongwen.表 import 顯示
        from pathlib import Path
        台積電財報 =  Path(__file__).parent / r'tifrs-fr1-m1-ci-cr-2330-2025Q1.html'
        df = 取現流表(台積電財報)
        顯示(df)
        self.assertEqual(df.iloc[-1]['取得不動產、廠房及設備'], -330826730000)

    def test爬取損益表(self):
        from zhongwen.date import 迄每季, 取日期
        from twse_crawler.財報爬蟲 import 爬取損益表
        from twse_crawler.財報爬蟲 import 爬取資產負債表
        from twse_crawler.財報爬蟲 import 爬取現流表
        from zhongwen.表 import 顯示
        # df = 爬取損益表(迄每季(取日期('2019.1.1'))) 
        # df = 爬取損益表(迄每季(取日期('2022.1.1'))) 
        df = 爬取現流表(取日期('2025.6.30'))
        顯示(df)
        self.assertIn('股票代號', df.columns)

    def test_parse_SCI_5287(self):
        from twse_crawler.財報爬蟲 import 取損益表
        from zhongwen.表 import show_html
        from zhongwen.時 import 取日期, 取期間
        df = 取損益表(self.數字)
        self.assertTrue(df.columns.is_unique)
        self.assertEqual(df.loc[0, '股票代號'], '5287')
        self.assertEqual(df.loc[0, '財報類型'], '合併')
        self.assertEqual(df.loc[0, '財報日期'], 取日期('2024.6.30'))
        self.assertEqual(df.loc[0, '母公司業主（淨利／損）'], 362_227_000)
        df = 取損益表(self.數字, True)
        self.assertEqual(df.loc[0, '財報季別'], 取期間('2024Q2'))
        self.assertEqual(df.loc[0, '母公司業主（淨利／損）'], 191_929_000)
        show_html(df)

    def test_parse_SCI_1312(self):
        from twse_crawler.財報爬蟲 import 取損益表
        from zhongwen.pandas_tools import 重名加序
        from zhongwen.date import 取日期
        df = 取損益表(self.國喬)
        self.assertTrue(df.columns.is_unique)
        self.assertEqual(df.loc[0, '股票代號'], '1312')
        self.assertEqual(df.loc[0, '財報類型'], '合併')
        self.assertEqual(df.loc[0, '財報日期'], 取日期('2024.6.30'))
        self.assertEqual(df.loc[0, '營業利益（損失）'], -551_990_000)
	
    def test_CE(self):
        from twse_crawler.財報爬蟲 import 取權益變動表
        from zhongwen.date import 取日期
        from zhongwen.pandas_tools import show_html, 重名加序
        import pandas as pd
        df = 取權益變動表(self.bytes1)
        self.assertTrue(df.empty)
        df = 取權益變動表(self.bytes2)
        self.assertTrue(df.columns.is_unique)
        self.assertIn('期末餘額-股本合計', df.columns)
        df = 取權益變動表(self.bytes3)
        self.assertTrue(df.columns.is_unique)
        # self.assertIn('期末餘額-股本合計', df.columns)
        print(df.columns.to_list())

    def test_open_report(self):
        from twse_crawler.財報爬蟲 import 財報檔
        from 股票分析.股票資訊查詢 import 股票代號
        財報檔(股票代號('鈊象'))

    def test取財報檔(self):
        from twse_crawler.財報爬蟲 import 取財報檔
        財報檔 = 取財報檔()

    def test下載季報包(self):
        from twse_crawler.財報爬蟲 import 下載季報包, 爬取損益表, 爬取資產負債表
        from twse_crawler.財報爬蟲 import 爬取損益表, 爬取資產負債表, 爬取現流表
        from twse_crawler.財報爬蟲 import 爬取權益變動表
        from twse_crawler.財報爬蟲 import 爬取財報
        from zhongwen.時 import 上季, 取期間
        from pandas import Period, period_range
        指定季度 = 取期間(f'2025Q2')
        下載季報包(2025, 2, 重新下載=True)
        return
        爬取損益表(指定季度)
        爬取資產負債表(指定季度)
        qs = period_range('2024', '2025', freq='Q')

        for q in qs:
            指定季度 = q
            print(q)
            下載季報包(q, 重新下載=True)
            爬取損益表(指定季度)
            爬取資產負債表(指定季度)
    
    def test爬取財報電子書(self):
        from twse_crawler.財報爬蟲 import 爬取財報電子書
        import os
        pdf = 爬取財報電子書('好樂迪', 2025, 2)
        cmd = f'start {pdf}'
        os.system(cmd)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    unittest.main()

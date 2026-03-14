import unittest

class Test(unittest.TestCase):

    def test抓上市公司基本資料(self):
        from 股票分析.證交所爬蟲 import 抓上市公司基本資料
        from zhongwen.表 import 顯示, 檢核欄位資料型態是否為字串

        df = 抓上市公司基本資料()
        print(df.columns)
        self.assertTrue(檢核欄位資料型態是否為字串(df, '公司代號'))
        顯示(df) 

    def test_parse_json(self):
        from 股票分析.證交所爬蟲 import cache
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.表 import 顯示
        from zhongwen.時 import 取今日, 取日期
        import pandas as pd
        from pathlib import Path
        import json
        j = Path(__file__).parent / '上市每日收盤行情測例.json'
        with j.open('r') as f: 
            j = json.load(f)

        for t in j['tables']:
            if '每日收盤行情' in t['title']:
                df = pd.DataFrame(t['data'], columns=t['fields'])
                break

    def test抓取上市每日收盤行情(self):
        from twse_crawler.證交所爬蟲 import 抓取上市每日收盤行情
        from zhongwen.時 import 今日, 取日期, 最近工作日
        from zhongwen.表 import 表示
        df =  抓取上市每日收盤行情(最近工作日)
    
        # 刪除指定名稱快取(cache, '抓取上市個股日成交資訊')
        # 刪除指定名稱快取(cache, '抓取上市每日收盤行情')
        # df = 抓取上市每日收盤行情(今日)
        表示(df)

    def test1(self):
        import pandas as pd
        p = pd.Period('2024-10')
        print(p)
        print(pd.Period(pd.to_datetime('2024Q1'), 'Q'))

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    # suite.addTest(Test('test抓上市公司基本資料'))
    suite.addTest(Test('test抓取上市每日收盤行情'))
    unittest.TextTestRunner().run(suite)

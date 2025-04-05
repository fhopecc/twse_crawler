import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test抓取股利分派情形彙總表(self):
        from twse_crawler.股利分派情形爬蟲 import 抓取股利分派情形彙總表, cache
        from zhongwen.快取 import 刪除指定名稱快取
        from zhongwen.文 import 刪空格
        from zhongwen.時 import 取期間, 本年度
        from zhongwen.表 import 顯示
        刪除指定名稱快取(cache, '抓取股利分派資料')
        df = 抓取股利分派情形彙總表(取期間(本年度).end_time.normalize())
        self.assertIn('本期淨利', df.columns)
        del df['摘錄公司章程-股利分派部分']
        del df['備註']
        顯示(df, 無格式=True)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test抓取股利分派情形彙總表'))  # 指定測試
    unittest.TextTestRunner().run(suite)

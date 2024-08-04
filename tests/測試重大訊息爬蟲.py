from unittest.mock import patch
import unittest

class Test(unittest.TestCase):

    @patch('zhongwen.file.抓取')
    def test_by_mock(self, 仿抓取):
        from pathlib import Path
        from twse_crawler.重大訊息爬蟲 import 爬取重大訊息
        from zhongwen.pandas_tools import show_html
        仿抓取.return_value = (Path(__file__).parent / 'test_t05st02.html').read_text()
        df = 爬取重大訊息('113.7.24') 
        self.assertEqual(df.loc[0, "公司名稱"], '華興')
        self.assertEqual(str(df.發言日期.dtypes), 'datetime64[ns]')
        self.assertEqual(str(df.歸屬日期.dtypes), 'datetime64[us]')
        self.assertIn('訊息', df.columns)
        show_html(df) 

    def test(self):
        from twse_crawler.重大訊息爬蟲 import 爬取重大訊息
        from zhongwen.pandas_tools import show_html
        df = 爬取重大訊息('113.7.24') 
        self.assertEqual(df.loc[0, "公司名稱"], '華興')
        self.assertEqual(str(df.發言日期.dtypes), 'datetime64[ns]')
        self.assertEqual(str(df.歸屬日期.dtypes), 'datetime64[us]')
        self.assertIn('訊息', df.columns)
        show_html(df)

    def test_rescratch(self):
        from zhongwen.date import 明天
        from twse_crawler.重大訊息爬蟲 import 爬取重大訊息
        with self.assertRaises(RuntimeError):
            爬取重大訊息(明天()) 

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test_by_mock'))  # 指定測試
    unittest.TextTestRunner().run(suite)

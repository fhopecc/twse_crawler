from pathlib import Path
from unittest.mock import patch
import unittest

class Test(unittest.TestCase):
    '依方法名稱字母順序測試'

    def test分析損益趨勢(self):
        from 股票分析.淨利趨勢分析 import 分析損益趨勢
        from 股票分析.損益表分析 import cache
        from zhongwen.表 import 顯示, 數據不足
        # cache.clear()
        self.maxDiff = None

        損益表分析結果 = r = 分析損益趨勢('志超')
        評語 = '114年第2季淨利轉為年減25%，為108年3月以來第2低，主要係業外由去年同季轉為損失；另營利年增7%，係費用年減16%，而營收僅年減2%，營利率增加所致，應分析月營收趨勢'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-61, r.分數)

        損益表分析結果 = r = 分析損益趨勢('好樂迪')
        評語 = '114年第2季由去年同季轉為淨損，連增2季，主要係業外由去年同季轉為損失；另營利年減32%，係營收年減13%，雖費用年減13%，仍不足彌補營收減少缺口所致，應分析月營收趨勢'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)


        損益表分析結果 = r = 分析損益趨勢('三地開發')
        評語 = '114年第2季淨損轉為年減17%，主要係業外損失年減23%；另營損年增45%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('華碩')
        評語 = '114年第2季淨利轉為年減17%，主要係營利年減32%，係成本年增39%，而營收僅增30%所致，應分析月營收趨勢；另業外利益年增1%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-42, r.分數)

        損益表分析結果 = r = 分析損益趨勢('新麥')
        評語 = '114年第2季淨利年減24%，連減2季，主要係業外利益年減79%；另營利年減2%，係成本年增8%，雖費用年減10%，仍不足彌補成本增加缺口'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-119, r.分數)

        損益表分析結果 = r = 分析損益趨勢('泰銘') 
        評語 = '114年第2季淨利年減96%，季減94%，連減2季，為108年3月以來第2低，主要係業外由去年同季轉為損失；另營利年減33%，係營收年減10%，雖成本年減5%，仍未能補足營收減少之缺口，應分析月營收趨勢'
        self.assertEqual(r.評語, 評語)

        損益表分析結果 = r = 分析損益趨勢('台榮')
        評語 = '114年第2季由去年同季轉為淨損，連增3季，主要係營利年減91%，係成本年增10%，而營收年減2%所致，應分析月營收趨勢；另業外利益年減60%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('互動')
        評語 = '114年第2季淨利年減23%，連減3季，主要係營利年減28%，係營收年減24%，雖成本年減28%，仍未能補足營收減少之缺口，應分析月營收趨勢；另業外利益年增74%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-173, r.分數)

        損益表分析結果 = r = 分析損益趨勢('京元電子')
        評語 = '114年第2季淨利年增14%，連增8季，為108年3月以來第3高，主要係營利年增91%，係營收年增94%，成本亦隨增96%，應分析月營收趨勢；另業外由去年同季轉為利益'
        self.assertEqual(評語, r.評語)
        self.assertEqual(286, r.分數)

        損益表分析結果 = r = 分析損益趨勢('宏和')
        評語 = '114年第2季淨利年減50%，連減3季，主要係營利年減38%，係營收年減37%，雖成本年減37%，仍未能補足營收減少之缺口，應分析月營收趨勢；另業外損失年增3,289%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-371, r.分數)

        損益表分析結果 = r = 分析損益趨勢('醣聯')
        評語 = '114年第2季淨損轉為年增2%，主要係其他(所得稅或停業單位)損失年增4,458%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('佳和')
        評語 = '114年第2季由去年同季轉為淨損，連增2季，主要係營利年減59%，係成本年增8%，雖費用年減13%，仍不足彌補成本增加缺口；另業外損失年增106%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('逸達')
        評語 = '114年第2季淨損年增100%，連增4季，為108年3月以來第3高，主要係營損年增98%，係成本年增53%；另業外由去年同季轉為損失'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('漢唐')
        評語 = '114年第2季淨利年增17%，連增4季，主要係營利年增127%，係營收年增30%，成本亦隨增18%，應分析月營收趨勢；另業外由去年同季轉為損失'
        self.assertEqual(評語, r.評語)
        self.assertEqual(172, r.分數)

        損益表分析結果 = r = 分析損益趨勢('全域')
        評語 = '114年第2季淨損轉為年增，由上季轉為淨損，為108年3月以來第2高，主要係業外由去年同季轉為損失；另營損年減41%，係營收年增269%，成本亦隨增272%，應分析月營收趨勢'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)
 
        損益表分析結果 = r = 分析損益趨勢('三一東林')
        評語 = '114年第2季淨損轉為年增11%，主要係業外損失年增46,111%；另營損年減35%，係營收年增61%，成本亦隨增84%，應分析月營收趨勢'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

        損益表分析結果 = r = 分析損益趨勢('益得')
        評語 = '114年第2季淨損轉為年減64%，季減62%，創108年3月以來26季新低，主要係營損年減76%，係營收年增1,586%，成本亦隨增34%，應分析月營收趨勢；另業外損失年增33%'
        self.assertEqual(評語, r.評語)
        self.assertEqual(-5000, r.分數)

    def test評價股票(self):
        from 股票分析.股票評價 import 評價股票
        import zhongwen.快取
        zhongwen.快取.停止快取=True
        r = 評價股票('鈊象') 
        print(r.評語)

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('googleclient').setLevel(logging.CRITICAL)
    logging.getLogger('matplotlib').setLevel(logging.CRITICAL)
    logging.getLogger('faker').setLevel(logging.CRITICAL)
    # 建構營收分析彙總表測例()
    # unittest.main()
    suite = unittest.TestSuite()
    suite.addTest(Test('test評價股票')) 
    unittest.TextTestRunner().run(suite)

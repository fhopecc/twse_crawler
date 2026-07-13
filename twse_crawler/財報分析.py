'''財報係依法公開財務報告，包含資產負債表、損益表及現流表'''
from zhongwen.快取 import 增加快取時序分析結果, 快取至記憶體
from zhongwen.程式 import 通知執行時間
from diskcache import Cache, Index
from twse_crawler import 財報爬蟲
from pathlib import Path
import functools
import logging
logger = logging.getLogger(Path(__file__).stem)
cache = Cache(Path.home() / '.twse_crawler' / '快取' / Path(__file__).stem)

@快取至記憶體
@通知執行時間
@cache.memoize('取財報彙總表', expire=12*60*60)
def 取財報彙總表(股票=None, 個體報表=False):
    '''
    一、主鍵：股票代號、財報類型、財報日期。
    二、資產負債表、損益表及現流表。
    三、預設僅包含個別及合併等財報類型者，啟用個體報表選項財傳回個體財報。
    四、依財報日期先後排序。
    五、損益表欄位：營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益、股數、
                    毛利率、營利率、稅前淨利率、淨利率。
    六、資產負債表欄位：資產總計、流動資產合計、
                        現金及約當現金、客戶保證金專戶、
                        合約資產－流動、應收票據淨額、應收帳款淨額、
                        存貨、
                        非流動資產合計、投資、
                        透過其他綜合損益按公允價值衡量之金融資產－非流動、
                        無形資產、其他非流動資產、 在建工程、
                        負債及權益總計、負債總計、
                        流動負債合計、短期借款、應付短期票券、
                        合約負債－流動、應付票據、應付帳款、
                        其他應付款、本期所得稅負債、
                        負債準備－流動、租賃負債－流動、其他流動負債、
                        非流動負債合計、一年或一營業週期內到期長期負債、
                        長期借款、應付公司債、其他非流動負債、
                        股本合計、權益總額、權益總計
    七、現流表欄位：營運產生之現金流入（流出）、營業活動之淨現金流入（流出）、
                    折舊費用、攤銷費用、
                    取得不動產、廠房及設備、投資活動之淨現金流入（流出）、
                    籌資活動之淨現金流入（流出）、
                    匯率變動對現金及約當現金之影響、本期現金及約當現金增加（減少）數
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號, 查股票簡稱
    from twse_crawler.資產負債表分析 import 取資產負債表
    from twse_crawler.損益表分析 import 取損益表
    from twse_crawler.現流表分析 import 取現流表
    from zhongwen.庫 import 批次載入
    from zhongwen.表 import 顯示

    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取財報彙總表(個體報表=個體報表)
        h = df.query('股票代號==@股票代號')
        h['財報季度索引'] = h.財報季度
        h = h.set_index('財報季度索引')
        return h.sort_index()

    df1 = 取資產負債表(個體報表=個體報表)
    df2 = 取損益表(個體報表=個體報表)
    df3 = 取現流表(個體報表=個體報表)

    df = df2.merge(df1
         ,on=['股票代號','財報日期','財報類型']
         )

    df = df.merge(df3
         ,on=['股票代號','財報日期','財報類型', '財報期別', '頻率']
         )

    df = df.sort_values('財報日期')
    return df

@functools.cache
@通知執行時間
@cache.memoize('取移動年度財報彙總表', expire=12*60*60)
def 取移動年度財報彙總表(股票=None, 個體報表=False):
    '''
    二、欄位：營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益
              營運產生之現金流入（流出）、營業活動之淨現金流入（流出）、
              取得不動產、廠房及設備、投資活動之淨現金流入（流出）、
              籌資活動之淨現金流入（流出）、
              匯率變動對現金及約當現金之影響、本期現金及約當現金增加（減少）數、
              資產總計、流動資產合計、現金及約當現金、應收帳款淨額、存貨、
              非流動資產合計、投資、透過其他綜合損益按公允價值衡量之金融資產－非流動、
              無形資產、其他非流動資產、 在建工程、
              負債及權益總計、負債總計、流動負債合計、合約負債－流動、客戶保證金專戶、應付帳款、
              本期所得稅負債、其他應付款、負債準備－流動、租賃負債－流動、其他流動負債、
              非流動負債合計、股本合計、權益總額、權益總計、
              母公司暨子公司所持有之母公司庫藏股股數（單位：股）
    二、主鍵：股票代號、財報類型、財報日期。
    三、季末資產負債表、近年損益表及現流表。
    '''
    from twse_crawler.股票基本資料分析 import 查股票代號
    from twse_crawler.資產負債表分析 import 取資產負債表
    from twse_crawler.損益表分析 import 取移動年度損益表
    from twse_crawler.現流表分析 import 取移動年度現流表
    from zhongwen.表 import 表示
    
    if 股票:
        股票代號 = 查股票代號(股票)
        df = 取移動年度財報彙總表(個體報表=個體報表)
        return df.query('股票代號==@股票代號')

    df1 = 取資產負債表(個體報表=個體報表)
    df2 = 取移動年度損益表(個體報表=個體報表)
    df3 = 取移動年度現流表(個體報表=個體報表)
    df = df2.merge(df1
         ,on=['股票代號','財報日期','財報類型']
         )

    df = df.reset_index(drop=True if df.index.name == '股票代號' or '股票代號' in df.index.names else False)
    df3 = df3.reset_index(drop=True if df3.index.name == '股票代號' or '股票代號' in df3.index.names else False)

    df = df.merge(df3
         ,on=['股票代號','財報日期','財報類型', '財報期別', '頻率']
         )

    df = df.sort_values('財報日期')
    return df


@functools.cache
@通知執行時間
@cache.memoize('取近年財報彙總表', expire=12*60*60)
def 取近年財報彙總表(股票, 個體報表=False):
    '''
    一、季末資產負債表、近年損益表及現流表。
    二、欄位：股票代號、財報類型、財報日期、
              營收、毛利、成本、營利、費用、稅前淨利、淨利、其他損益
              資產總計、流動資產合計、現金及約當現金、應收帳款淨額、存貨、
              非流動資產合計、投資、透過其他綜合損益按公允價值衡量之金融資產－非流動、
              無形資產、其他非流動資產、 在建工程、
              負債及權益總計、負債總計、流動負債合計、合約負債－流動、客戶保證金專戶、應付帳款、
              本期所得稅負債、其他應付款、負債準備－流動、租賃負債－流動、其他流動負債、
              非流動負債合計、股本合計、權益總額、權益總計、
              母公司暨子公司所持有之母公司庫藏股股數（單位：股）
    '''
    try:
        return 取移動年度財報彙總表(股票).iloc[-1]
    except IndexError as e:
        raise IndexError(f'{股票}無足夠財報彙總表資料：{e}')
        

def 依資料日期距今月數打折分數(分數, 資料日期):
    from zhongwen.時 import 今日
    import pandas as pd
    import math
    if pd.isna(資料日期):
        資料日期 = 今日
    與資料日期相距月數 = math.floor((今日 - 資料日期).days / 30)
    return 分數*0.8**與資料日期相距月數

def 彙總分析(股票, 分析項目):
    '''
    一、彙總指定分析項目結果。
    二、項目：分數、評語，其中分數係各項分數加總，
        評語係各項評語以分號「；」連接。
    '''
    from zhongwen.表 import 顯示, 數據不足
    from collections.abc import Iterable 
    import pandas as pd
    if isinstance(分析項目, str) or not isinstance(分析項目, Iterable):
        分析項目 = [分析項目]
    rs = []
    for f in 分析項目:
        try:
            rs.append(f(股票))
        except 數據不足 as e:
            logger.error(e)
            continue
        except IndexError as e:
            logger.error(f'{股票}計算{f.__name__}發生錯誤{e}!')
            continue
    if len(rs)==0: 
        return pd.Series()
    else:
        ss = pd.concat(rs)
        if ss.empty:
            return pd.Series()
    rs = pd.DataFrame(rs).dropna(subset=['評語']).reset_index(drop=True)
    rs = rs[['分數', '評語']].sort_values(by='分數', key=abs, ascending=False)
    r = pd.Series()
    r['分數'] = rs['分數'].sum()
    def 評語在其他評語中(評語, 評語組):
        for r in 評語組:
            if len(評語) < len(r) and 評語 in r:
                return True
        return False

    r['評語'] = '；'.join(r for r in sorted(rs['評語'].astype(str), key=len) 
                             if len(r)>0 and not 評語在其他評語中(r, rs['評語'])
                         )
    return r 

def 杜邦分析(股票):
    '''
    一、分析結果：分數、評語、除錯訊息
    二、分數 = min( ( ROE-0.09 )/0.02, 20)
    三、如淨利等流動數據係取近4季累積值，如股東權益等結構數據係取本季末值。
    '''
    from twse_crawler.趨勢分析 import 分析歷季數據增減情形, 分析同比差異主次因
    from twse_crawler.淨利趨勢分析 import 分析損益趨勢
    from twse_crawler.淨利分析 import 分析淨利率
    from zhongwen.時 import 取民國季度, 今日
    from zhongwen.數 import 最簡約數, 取增減數
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    歷季財報 = 取財報彙總表(股票)
    if 歷季財報.empty:
        r = pd.Series()
        r['分數'] = 0
        r['評語'] = ''
        r['財報日期'] = 今日
        return r
    財報日期 = 歷季財報.財報日期.iloc[-1]
    淨利 = 歷季財報.淨利.rolling(window=4).sum()
    營收 = 歷季財報.營收.rolling(window=4).sum()
    資產 = 歷季財報.資產總計
    歷季財報['權益總計'] = 歷季財報.權益總計.fillna(歷季財報.權益)
    股東權益 = 歷季財報.權益總計
    歷季財報['股東權益報酬率'] = 股東權益報酬率 = 淨利 / 股東權益
    歷季財報['淨利率'] = 淨利率 = 淨利 / 營收
    歷季財報['資產周轉率'] = 資產周轉率 = 營收 / 資產
    歷季財報['權益乘數'] = 權益乘數 = 資產 / 股東權益
    本季財報 = 歷季財報.iloc[-1]
    # 顯示(歷季財報)
    s, msg, log = 0, '', ''
    r = 分析歷季數據增減情形(歷季財報, '股東權益報酬率', '財報日期'
                            ,表達增減百分點=True
                            ,表達同比=False
                            )
    同比差異主因 = '淨利率' # 預設分析淨利率
    if not r.empty:
        msg = r.說明
        if 歷季財報.shape[0]>20:
            s += int(min(max((r.歷史最小數-0.09)/0.02, 0), 20))
        r2 = 分析同比差異主次因(歷季財報, 股票, '股東權益報酬率'
                               ,['淨利率', '資產周轉率', '權益乘數']
                               ,子項結合方式='積'
                               )
        log += r2.除錯訊息
        if not r2.empty:
            同比差異主因 = r2.主因
        r3 = pd.Series()
        if 同比差異主因 == '淨利率':
            r3 = 分析淨利率(股票)
        elif 同比差異主因 == '資產周轉率':
            r3 = 分析資產周轉率(股票)
        elif 同比差異主因 == '權益乘數':
            r3 = 分析權益乘數(股票)
        if not r3.empty:
            if 同比差異主因 == '淨利率':
                msg += f'，主要係{r3.說明}'
            else:
                msg += f'，主要係{r3.說明}，使{同比差異主因}{取增減數(r2.主因差異值*100)}個百分點'
                if 同比差異主因 == '權益乘數':
                    msg += f'至{本季財報[同比差異主因]:.2f}'
                else:
                    msg += f'至{本季財報[同比差異主因]:.0%}'
    msg += (f'，{m3}' if len(m3:='，'.join(
        f'{m2}{本季財報[m2]:.2f}' if m2 == '權益乘數' else f'{m2}{本季財報[m2]:.0%}' for m2 in 
        sorted([m for m in ['淨利率', '資產周轉率', '權益乘數'] if 同比差異主因 not in m]
              ,key=lambda c: -本季財報[c]
              ))) > 0 else ''
           )
    r = pd.Series()
    r['財報日期'] = 財報日期
    r['分數'] = s
    r['評語'] = f'{msg}({s:.0f})'
    r['除錯訊息'] = log
    return r

def 分析資產周轉率(股票):
    '''
    一、分析結果：說明。
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    from twse_crawler.營收分析 import 分析月營收
    from twse_crawler.損益表分析 import 分析營收
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    歷季財報['營收'] = 歷季財報.營收.rolling(window=4).sum()
    if len(歷季財報) < 5: return pd.Series()
    msg = ''
    r1 = 分析同比差異主次因(歷季財報, '股票', '資產周轉率'
                           ,['營收', '資產總計']
                           ,子項結合方式='分數'
                           )
    if not r1.empty:
        if r1.主因 == '營收':
            r2 = 分析營收(股票)
        elif r1.主因 == '資產總計':
            r2 = 分析資產(股票)
        else:
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False, 表達本季數值=False
                                     ,表達同比=True)

        if not r2.empty:
            msg += f'{r2.說明}'
        else:
            msg += f'{r1.主因}'
    r = pd.Series()
    if len(msg) > 0:
        r['說明'] = msg
    return r

def 分析權益乘數(股票):
    '''
    一、分析結果：說明。
    二、權益乘數=資產總計/權益總計
    三、同比差異主因如係資產總計，顯示資產差異係負債差異所致，應分析負債。
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()
    msg = ''
    r1 = 分析同比差異主次因(歷季財報, '股票', '權益乘數'
                           ,['資產總計', '權益總計']
                           ,子項結合方式='分數'
                           )
    if not r1.empty:
        if r1.主因 == '資產總計': # 資產差異係負債差異所致，應分析負債
            r2 = 分析負債(股票)
            if not r2.empty:
                msg += f'{r2.說明}'
        else: # 資產差異係權益差異所致
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False, 表達本季數值=False
                                     ,表達同比=True)
            if not r2.empty:
                msg += f'{r2.說明}'
            else:
                msg += f'{r1.主因}'
    r = pd.Series()
    if len(msg) > 0:
        r['說明'] = msg
    return r

def 分析資產(股票):
    '''
    一、分析結果：說明。
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()
    msg = ''
    r1 = 分析同比差異主次因(歷季財報, 股票, '資產總計' 
                           ,['流動資產合計', '非流動資產合計']
                           )
    r = 分析歷季數據增減情形(歷季財報, '資產總計', '財報日期')
    if not r1.empty:
        if r1.主因 == '流動資產合計':
            msg += 分析流動資產(股票).說明
        else:
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False
                                     ,表達本季數值=False
                                     ,表達同比=True
                                     )
            if not r2.empty:
                msg += f'{r2.說明}'
            else:
                msg += f'主要係{r1.主因}'
    r = pd.Series()
    r['說明'] = msg
    return r

def 分析流動資產(股票):
    '''
    一、分析結果：說明。
    二、分析現金、應收帳款及存貨。
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    from twse_crawler.現流表分析 import 分析自由現金流
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()
    說明 = ''
    r1 = 分析同比差異主次因(歷季財報, 股票, '流動資產合計' 
                           ,['現金及約當現金', '應收帳款淨額', '存貨']
                           )
    if not r1.empty:
        if r1.主因 == '現金及約當現金':
            r2 = 分析自由現金流(股票)
        else:
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False
                                     ,表達本季數值=False
                                     ,表達同比=True
                                     )
        if not r2.empty:
            try:
                說明 += f'{r2.說明}'
            except AttributeError:
                說明 += f'{r2.評語}'
        else:
            說明 += f'主要係{r1.主因}'
    r = pd.Series()
    r['說明'] = 說明
    return r

def 取最近年度損益表科目金額(股票, 科目):
    '最近財報如為第4季即傳回本年度指定科目加總，否則傳回上年度加總。'
    歷季損益表 = 取財報彙總表(股票)
    if len(歷季損益表) < 5: 0
    c = 歷季損益表.iloc[-1]
    年度 = c.財報日期.year
    if c.財報日期.month < 12:
        年度 -= 1
    y = 歷季損益表.query('財報日期.dt.year==@年度')
    return y[科目].fillna(0).sum()

def 取近四季損益表科目金額(股票, 科目):
    '''
    一、資料不足4季則取季平均值乘4，稱平均年化值。
    '''
    歷季損益表 = 取財報彙總表(股票)
    金額 = 歷季損益表[科目].tail(4).mean()*4
    return 金額

def 分析合約負債(股票):
    '''
    一、分析結果：分數(10%)、評語、流動合約負債占近四季營收比率
    二、最近年營收係近四季營收。
    三、依重要性僅分析流動合約負債占近四季營收比率大於3成者。
    四、分數為年化增減趨勢分數乘流動合約負債占近四季營收比率，
        以同時反映合約負債增減趨勢及規模之影響。
    '''
    from twse_crawler.趨勢分析 import 分析歷季數據增減情形, 年化趨勢評分
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()

    t = 最近財報 = 歷季財報.iloc[-1]
    近四季營收 = 取近四季損益表科目金額(股票, '營收')
    流動合約負債占近四季營收比率 = t["合約負債－流動"]/近四季營收
    評語 = ''
    if 流動合約負債占近四季營收比率 > 0.3:
        res = 流動合約負債增減分析結果 = \
                  分析歷季數據增減情形(歷季財報
                                      ,'合約負債－流動'
                                      ,'財報日期'
                                      ,數據名稱='流動合約負債'
                                      ,表達數據季別=False
                                      ,表達數據名稱=False
                                      )
        if not res.empty:
            if res.同比 > 0:
                分數 = ( res.同比*abs(res.連續增減次數) ) / 0.1
                分數 *= 流動合約負債占近四季營收比率
                評語 = f'流動合約負債占近四季營收比率達{流動合約負債占近四季營收比率:.0%}'
                評語 += f"，金額{res.說明}"

    r = 合約負債評分結果 = pd.Series()
    if 評語 != '':
        r['流動合約負債占近四季營收比率'] = 流動合約負債占近四季營收比率
        r['評語'] = 評語
        r['分數'] = int(min(分數, 10))
    return r

def 分析存貨(股票):
    '''
    一、分析結果：最近存貨金額、比重、增減、周轉天數及其增減、評語及分數
    二、僅分析存貨比重達1成者。
    三、存貨比重達3成者未滿4成扣100分，每增逾1成增扣100分，最高扣500分，
        以反映存貨比重過高衍生之資金積壓風險。
    四、周轉天數增加，存貨減少反映銷售狀況惡化，扣500分。
    五、周轉天數趨勢增減百分點增減100分。
    '''
    from twse_crawler.趨勢分析 import 分析歷季數據增減情形, 年化趨勢評分
    from zhongwen.表 import 顯示
    import pandas as pd
    import numpy as np
    歷季財報 = 取財報彙總表(股票)
    歷季財報['近四季營業成本合計'] = 歷季財報.營業成本合計.rolling(window=4).sum()
    歷季財報['存貨周轉天數'] = 365/(歷季財報.近四季營業成本合計/歷季財報.存貨)

    c = 最近資訊負債表 = 歷季財報.iloc[-1]
    r = 存貨分析結果 = pd.Series()

    股票代號 = c.股票代號
 
    存貨比重 = c.存貨 / c.負債及權益總計

    評語 = ''
    if 存貨比重 > 0.1:
        評語 = f'存貨比重達{存貨比重:.0%}'
        bins =   [-np.inf,  0.3,  0.4, 0.5, 0.6, 0.7,np.inf]
        labels = [      0, -100, -200,-300,-400,-500]
        分數 = pd.cut([存貨比重], bins=bins, labels=labels, right=False, include_lowest=True)[0]
        try:
            存貨增減情形 = \
                分析歷季數據增減情形(歷季財報
                                    ,'存貨'
                                    ,'財報日期'
                                    ,數據名稱='存貨'
                                    ,表達數據季別=False
                                    )
            res = 存貨周轉天數增減情形 = 分析歷季數據增減情形(
                                         歷季財報
                                        ,'存貨周轉天數'
                                        ,'財報日期'
                                        ,數據名稱='周轉天數'
                                        ,數據單位='天'
                                        ,表達數據季別=False
                                        )

            評語 += f"，{存貨周轉天數增減情形.說明}"

            # 分數乘負一係周轉天數增加反映營運不佳，減少反映營運較佳。
            分數 += -1*年化趨勢評分(res.本季, res.同比, res.連續增減次數, 增減百分點乘數=100) 
            try:
                分數 = min(500, max(-500, 分數))
            except TypeError:
                分數 = 0 # 如是NA的情形。

            周轉天數連續增減次數 = 存貨周轉天數增減情形.連續增減次數 
            存貨連續增減次數 = 存貨增減情形.連續增減次數 
            if 周轉天數連續增減次數 < 0 and 存貨連續增減次數 > 0:
                評語 += f"，惟{存貨增減情形.說明}，顯示銷貨增長強勁"
                分數 *= 2
            elif 周轉天數連續增減次數 < 0 and 存貨連續增減次數 < 0:
                評語 = f"{存貨增減情形.說明}，比重仍達{存貨比重:.0%}，且{存貨周轉天數增減情形.說明}，顯示產能不足銷貨增長需求"
            elif 周轉天數連續增減次數 > 0 and 存貨連續增減次數 > 0:
                評語 += f"，滯銷致{存貨增減情形.說明}"
            elif 周轉天數連續增減次數 > 0 and 存貨連續增減次數 < 0:
                評語 += f"，惟{存貨增減情形.說明}，顯示銷貨減少嚴重"
                分數 = -500
        except AttributeError: 
            logger.error(f'{股票}無法分析歷季數據增減情形！')
        except ValueError:
            logger.error(f'{股票}無法進行年化趨勢評分！')
    if 評語 != '':
        r['評語'] = 評語 
        r['分數'] = int(分數) 
    else:
        r = pd.Series()
    return r

def 取歷年營收占台積電資本資出比例(主營台積電建廠服務者):
    from twse_crawler.現流表分析 import 取年度現流表
    from twse_crawler.損益表分析 import 取年度損益表
    from twse_crawler.自結損益 import 預測前年至次年周期數據
    from zhongwen.表 import 顯示
    df1 = 取年度現流表('台積電')
    df1 = df1[['財報日期', '取得不動產、廠房及設備']]
    df1 = df1.rename(columns={'取得不動產、廠房及設備':'台積電資本資出'})
    df1['台積電資本資出'] = df1.台積電資本資出.abs()
    df2 = 取年度損益表(主營台積電建廠服務者)
    df2 = df2.merge(df1, on='財報日期')
    df2['營收占台積電資本資出比例'] = df2.營收/df2.台積電資本資出.abs()
    return df2

def 分析負債(股票):
    '''
    一、分析結果：說明。
    二、負債=流動負債合計+非流動負債合計
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()
    msg = ''
    r1 = 分析同比差異主次因(歷季財報, 股票, '負債總計' 
                           ,['流動負債合計', '非流動負債合計']
                           )
    r = 分析歷季數據增減情形(歷季財報, '負債總計', '財報日期')
    if not r1.empty:
        if r1.主因 == '流動負債合計':
            msg += 分析流動負債(股票).說明
        else:
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False
                                     ,表達本季數值=False
                                     ,表達同比=True
                                     )
            if not r2.empty:
                msg += f'{r2.說明}'
            else:
                msg += f'主要係{r1.主因}'
    r = pd.Series()
    r['說明'] = msg
    return r

def 分析流動負債(股票):
    '''
    一、分析結果：說明。
    二、流動負債=合約負債－流動+應付帳款+本期所得稅負債+其他應付款
                +負債準備－流動+租賃負債－流動+其他流動負債	

    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析歷季數據增減情形
    from twse_crawler.現流表分析 import 分析自由現金流
    import pandas as pd
    歷季財報 = 取財報彙總表(股票)
    if len(歷季財報) < 5: return pd.Series()
    說明 = ''
    r1 = 分析同比差異主次因(歷季財報, 股票, '流動負債合計' 
                           ,['合約負債－流動', '應付帳款'
                            ,'本期所得稅負債'
                            ,'其他應付款', '負債準備－流動', '租賃負債－流動', '其他流動負債' 
                            ]
                           )
    if not r1.empty:
        if r1.主因 == '合約負債－流動':
            r2 = pd.Series()
        else:
            r2 = 分析歷季數據增減情形(歷季財報, r1.主因, '財報日期'
                                     ,表達數據季別=False
                                     ,表達同比=True
                                     )
        if not r2.empty:
            try:
                說明 += f'{r2.說明}'
            except AttributeError:
                說明 += f'{r2.評語}'
        else:
            說明 += f'{r1.主因}增減'
    r = pd.Series()
    r['說明'] = 說明
    return r

def 分析應付帳款(股票):
    歷季財報 = 取財報彙總表(股票)

def 分析現金轉換周期(股票):
    '''
    一、現金循環次數配5分，產業排名情形配5分。
    二、存貨周轉次數原係成本除以存貨，調整為營收除以存貨，
        可避免如鈊象，其權利金占營收比更高，卻因商用遊戲機存貨周轉次數少，
        而高估其現金轉換周期，
        以營收取代成本可一併考量銷貨占營收比。
    '''
    from zhongwen.表 import 顯示
    import pandas as pd
    r = 取近年財報彙總表(股票)    
    if r is None:
        logger.error(f"{股票}無近年財報彙總表")
        return pd.Series()

    if r.營利 > 0:
        if r.稅前淨利 > 0:
            r['營業比重']= r.營利 / r.稅前淨利
        else:
            r['營業比重']= 1
    else:
        r['營業比重']= 0

    r['成本費用'] = r.成本 + r.費用
    r['存貨周轉次數'] = r.營收 / r.存貨 
    r['應收帳款周轉次數'] = r.營收 / r.應收帳款淨額 
    r['應付帳款周轉次數'] = r.成本費用 / r.應付帳款
    r['存貨周轉天數'] = 365 / r.存貨周轉次數
    r['應收帳款周轉天數'] = 365 / r.應收帳款周轉次數
    r['應付帳款周轉天數'] = 365 / r.應付帳款周轉次數
    r['現金循環天數'] = sum(r[項目]
                        for 項目 in ['存貨周轉天數', '應收帳款周轉天數'
                                    ]
                        if pd.notna(r[項目])
                        ) - r['應付帳款周轉天數'] if pd.notna(r['應付帳款周轉天數']) else 0

    r['分數'] = min(365/(1 if r.現金循環天數 <= 0 else r.現金循環天數)*r.營業比重, 5)

    r['評語']= '，'.join(f'{項目}{r[項目]:.0f}天' 
                        for 項目 in ['存貨周轉天數', '應收帳款周轉天數'
                                    ,'應付帳款周轉天數', '現金循環天數'
                                    ]
                        if pd.notna(r[項目])
                        ) + f'({r["分數"]:.0f})'
    return r

def 取自由現金流對淨利比(股票):
    '''
    一、取自由現金流對淨利比。
    '''
    import pandas as pd
    df = 取移動年度財報彙總表(股票)
    df['自由現金流對淨利比'] = df['自由現金流']/df.淨利
    return df.iloc[-1].自由現金流對淨利比

def 分析資產負債科目占比(股票):
    from twse_crawler.財報爬蟲 import 損益表使用欄位, 資產負債表使用欄位, 現流表使用欄位
    import pandas as pd
    r = 取財報彙總表(股票).iloc[-1]
    資產總計 = r.資產總計
    上級科目 = ['資產總計占比'
               ,'負債及權益總計占比', '負債總計占比', '流動負債合計占比']
    文字欄位 = ['股票代號', '財報類型', '財報季度']
    for c in [c for c in r.index if c not in 文字欄位+上級科目]:
        if c in 資產負債表使用欄位:
            r[f'{c}占比'] = pd.to_numeric(r[c]) / 資產總計
    占比欄位 = [c for c in r.index if '占比' in c]
    占比欄位 = [c for c in 占比欄位 if '總計' not in c and '總額' not in c and '合計' not in c]
    rs = r[占比欄位]
    rs = rs.sort_values(ascending=False, key=lambda x: pd.to_numeric(x, errors='coerce').fillna(0))   
    r['前五大科目'] = '、'.join([f'{i}({r:,.0%})' for i, r in zip(rs.iloc[:5].index, rs.iloc[:5].values)])
    return r

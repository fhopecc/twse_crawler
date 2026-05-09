def 分析淨利率(股票):
    '''
    一、淨利率 = 淨利 / 營收
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析本季同比, 分析歷季數據增減情形
    from twse_crawler.損益表分析 import 取損益表, cache
    歷季損益表 = 取損益表(股票)
    m, s = '', 0
    r = 分析歷季數據增減情形(歷季損益表, '淨利率', '財報日期'
                            ,表達增減百分點=True
                            )
    if not r.empty:
        m = r.說明
        r1 = 分析同比差異主次因(歷季損益表, 股票
                               ,'淨利率'
                               ,['淨利', '營收']
                               ,'分數'
                               )
        if not r1.empty:
            if r1.主因 == '淨利':
                r2 = 分析淨利(股票)
            elif r1.主因 == '營收':
                r2 = 分析本季同比(歷季損益表, '營收', '財報日期')
            if not r2.empty:
                m += f'，係{r2.說明}'
            else:
                m += f'，係{r1.主因}所致'
        r['說明'] = m
    return r

def 分析淨利(股票=None):
    '''
    一、淨利=稅前淨利+其他損益
    二、欄位：說明
    二、向舊相容欄位：評語、分數
    '''
    from twse_crawler.趨勢分析 import 分析同比差異主次因, 分析本季同比, 分析歷季數據增減情形
    from twse_crawler.損益表分析 import 取損益表, cache
    from zhongwen.表 import 顯示
    import pandas as pd
    # cache.clear()
    歷季損益表 = 取損益表(股票)
    m = ''
    s = 0
    r0 = 分析歷季數據增減情形(歷季損益表, '淨利', '財報日期')
    if not r0.empty:
        if r0.同比 > 0:
            s = min(( r0.同比*abs(r0.連續增減次數) ) / 0.1, 5)
        m = r0.說明
        r1 = 分析同比差異主次因(歷季損益表, 股票, '淨利',['稅前淨利', '其他損益'])
        if not r1.empty:
            if r1.主因 == '稅前淨利':
                r2 = 分析稅前淨利(股票)
            elif r1.主因 == '其他損益':
                r2 = 分析其他損益(股票)
            if not r2.empty:
                m += f'，係{r2.說明}'
            else:
                m += f'，係{r1.主因}所致'
        r0['分數'] = s
        r0['說明'] = m
        r0['評語'] = m
        r0['財報日期'] = 歷季損益表.iloc[-1].財報日期
    return r0

def 分析稅前淨利(股票):
    '''
    一、稅前淨利(項目)=營利+業外損益(子項)。
    二、如無營利直接分析稅前淨利同比。
    '''
    from twse_crawler.趨勢分析 import 分析本季同比, 分析同比差異主次因
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取增減百分比
    import pandas as pd
    分析結果 = pd.Series()
    歷季損益表 = 取損益表(股票)
    if 歷季損益表.營利.isna().all():
        r1 = 分析本季同比(歷季損益表, '稅前淨利', '財報日期')
        if not r1.empty:
            分析結果['說明'] = r1.說明
    else: # 有營利數據
        r1 = 分析同比差異主次因(歷季損益表, 股票, '稅前淨利'
                               ,['營利', '業外損益'])
        營利分析結果 = 分析營利(股票)
        業外收益分析結果 = 分析業外損益(股票)
        if r1.主因 == '營利':
            分析結果['說明'] = 營利分析結果.說明 + "，及"+ 業外收益分析結果.說明
        elif r1.主因 == '業外損益':
            分析結果['說明'] = 業外收益分析結果.說明 + "，及" + 營利分析結果.說明
    return 分析結果

def 分析其他損益(股票):
    '''
    一、結果項目：說明。
    二、
    '''
    from twse_crawler.趨勢分析 import 分析本季同比
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 顯示, 數據不足
    import pandas as pd
    分析結果 = pd.Series()
    歷季損益表 = 取損益表(股票)
    r = 分析本季同比(歷季損益表, '其他損益', '財報日期')
    分析結果['說明'] = r.說明
    return 分析結果

def 分析營利(股票):
    '''
    一、結果項目：說明。
    二、營利=毛利-費用
    '''
    from twse_crawler.趨勢分析 import 分析本季同比, 分析同比差異主次因
    from twse_crawler.損益表分析 import 取損益表, 分析營收
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取增減百分比
    import pandas as pd
    分析結果 = pd.Series()
    歷季損益表 = 取損益表(股票)
    歷季損益表['毛利'] = 歷季損益表.毛利.fillna(0)
    本季毛利 = 歷季損益表.毛利.iloc[-1]
    # 顯示(歷季損益表)
    if 本季毛利 == 0:
        歷季損益表['成本費用'] = 歷季損益表.營收 - 歷季損益表.營利
        r1 = 分析同比差異主次因(歷季損益表, 股票, '營利', ['營收', '成本費用'])
        if r1.主因 == '營收':
            r2 = 分析營收(股票)
        else:
            r2 = 分析本季同比(歷季損益表, '成本費用', '財報日期', 表達數據季別=False)
    else:
        r1 = 分析同比差異主次因(歷季損益表, 股票, '營利', ['毛利', '費用'])
        if r1.主因 == '毛利':
            r2 = 分析毛利(股票)
        else:
            r2 = 分析本季同比(歷季損益表, '費用', '財報日期', 表達數據季別=False)

    分析結果['說明'] = r2.說明
    return 分析結果

def 分析毛利(股票):
    '''
    一、結果項目：說明。
    二、毛利=營收-成本
    '''
    from twse_crawler.趨勢分析 import 分析本季同比, 分析同比差異主次因
    from twse_crawler.損益表分析 import 取損益表, 分析營收
    from zhongwen.表 import 顯示, 數據不足
    from zhongwen.數 import 取增減百分比
    import pandas as pd
    分析結果 = pd.Series()
    歷季損益表 = 取損益表(股票)
    歷季損益表['成本'] = 歷季損益表.成本.fillna(0)
    歷季損益表['毛利'] = 歷季損益表.毛利.fillna(歷季損益表.營收-歷季損益表.成本)
    r1 = 分析同比差異主次因(歷季損益表, 股票, '毛利', ['營收', '成本'])
    if r1.主因 == '營收':
        r2 = 分析營收(股票)
    else: # 成本
        r2 = 分析本季同比(歷季損益表, r1.主因, '財報日期', 表達數據季別=False)
        r2_1 = 分析本季同比(歷季損益表, '毛利率', '財報日期', 表達數據季別=False)
        r2['說明'] = r2_1.說明
        if r2_1.同比 < -0.1:
            r2['說明'] = f'{r2.說明}，應注意分析減少原因'
    分析結果['說明'] = f'{r2.說明}'
    return 分析結果

def 分析業外損益(股票):
    '''
    一、欄位：說明、本季、同比。
    '''
    from twse_crawler.趨勢分析 import 分析本季同比 
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 顯示
    import pandas as pd
    歷季損益表 = 取損益表(股票)
    r = 分析本季同比(歷季損益表, '業外損益', '財報日期', 表達數據季別=False)
    r['說明'] = f'{r.說明}'
    return r



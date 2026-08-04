from zhongwen.程式 import 通知執行時間
from diskcache import Cache, Index
from pathlib import Path

各股預估模型誤差率明細檔 = Index(str(
    Path.home() / '.twse_crawler' / r'資料庫/各股預估模型誤差率明細檔'
    ))


@通知執行時間
def 預估次年底淨利(股票, 重新評估模型=False):
    '''
    整體模型修正：
    刪除 swmpe，只用 wmpe 衡量，並將 wmpe 正名為 wmape，另
    y_原始 改為 y、y_訓練 改為 y_train、
    y_真實驗證 改為 y_validation、y_最終訓練 改為 y_best_train。
    '''
    import pandas as pd
    from twse_crawler.財報分析 import 取財報彙總表
    from twse_crawler.營收分析 import 預測次年底營收
    from twse_crawler.無腦預測至次年底每季值 import 無腦預估至次年底每季值
    from twse_crawler.預估至次年底每季值 import 預估至次年底每季值丙式
    from twse_crawler.預估至次年底每季值 import 表達預估說明丙
    from twse_crawler.月營收逐步推估淨利 import 以月營收逐步推估淨利
    h = 取財報彙總表(股票)
    try:
        if not 重新評估模型:
            # raise KeyError('重評')
            r = 各股預估模型誤差率明細檔[股票]
            min_row = r.loc[r.誤差率.idxmin()]
            best_model = min_row.模型
            # print(best_model)
            if '期值' in best_model:
                p = 無腦預估至次年底每季值(h.淨利)
                p['各項預估說明'] = f'【預估淨利】{表達預估說明丙(p)}'
            elif 'Theta' in best_model:
                p = 預估至次年底每季值丙式(h.淨利)
                p['各項預估說明'] = f'【預估淨利】{表達預估說明丙(p)}'
            elif '月營收逐步' in best_model:
                p = 以月營收逐步推估淨利(股票)
                p['各項預估說明'] = f'【預估淨利】{p.各項預估說明}{表達預估說明丙(p)}'
            # print(r)
            p['模型評估結果'] = r
            return p
    except KeyError: pass
    try:
        無腦預估結果 = 無腦預估至次年底每季值(h.淨利)
        西塔預估結果 = 預估至次年底每季值丙式(h.淨利)
        月營收逐步推估淨利結果 = 以月營收逐步推估淨利(股票)
        r = pd.DataFrame([
             {'模型': 無腦預估結果.模型名稱, '誤差率':無腦預估結果.誤差率}
            ,{'模型': 西塔預估結果.模型名稱, '誤差率':西塔預估結果.誤差率}
            ,{'模型': 月營收逐步推估淨利結果.模型名稱, '誤差率':月營收逐步推估淨利結果.誤差率}
            ])
        各股預估模型誤差率明細檔[股票] = r
        min_row = r.loc[r.誤差率.idxmin()]
        for m in [無腦預估結果, 西塔預估結果, 月營收逐步推估淨利結果]:
            if m.模型名稱 == min_row.模型:
                m['模型評估結果'] = r
                if hasattr(m, '各項預估說明'):
                    m['各項預估說明'] = f'【預估淨利】{m.各項預估說明}{表達預估說明丙(m)}'
                else:
                    m['各項預估說明'] = f'【預估淨利】{表達預估說明丙(m)}'
                return m
        return r
    except IndexError as e:
        from twse_crawler.股利分析 import 無法預估盈餘
        raise 無法預估盈餘(f'預測{股票}每股盈餘發生:{e}')

@通知執行時間
def 以淨利預測次年每股盈餘(股票):
    '''
    一、針對一零四型公司，逕以淨利預測前年至次年每股盈餘。
    二、預測結果：前年至次年每股盈餘、預估說明、預估方法說明。
    三、增加預測上季每股盈餘及實際每股盈餘差異。
    四、歷月營收表最近營收月份或歷季損益表最近財報季度大於快取對應值始更新。
    '''
    from twse_crawler.股票基本資料分析 import 查股票簡稱, 查股票代號, 取股票基本資料彙總表
    from twse_crawler.預估次年底 import 表達預估方法, 表達預估說明
    from twse_crawler.營收分析 import 取歷月營收表
    from zhongwen.快取 import 刪除指定名稱快取
    from twse_crawler.損益表分析 import 取損益表
    from zhongwen.表 import 表示
    from zhongwen.數 import 取最簡約數
    import pandas as pd
    公司簡稱 = 查股票簡稱(股票)
    公司代號 = 查股票代號(股票)

    歷季損益表 = 取損益表(公司代號)
    最近財報季度 = 歷季損益表.財報季度.max()
    
    # 預測淨利
    預估淨利結果 = 預估次年底淨利(股票)
    預測淨利 = 預估淨利結果.預估各季值
    future_index = 預測淨利.index[預測淨利.index > 最近財報季度]
    new_index = 歷季損益表.index.append(future_index)
    歷季損益表 = 歷季損益表.reindex(new_index)
    歷季損益表['淨利'] = 歷季損益表.淨利.fillna(預測淨利)

    # 預測每股盈餘
    q = 取股票基本資料彙總表(股票)
    股數 = q.股數.iloc[-1]
    歷季損益表['每股盈餘'] = 歷季損益表.每股盈餘.fillna(歷季損益表.淨利/股數)
    from zhongwen.時 import 前年至次年各季末
    前年至次年各季數據 = 歷季損益表.reindex(index=前年至次年各季末.to_period('Q'))
    年度每股盈餘 = 前年至次年各季數據.每股盈餘.resample('Y').sum()

    # 表達預估方法及預估結果
    from twse_crawler.預估次年底 import 移除重覆時間詞
    預估方法說明 = (f'{預估淨利結果.各項預估說明}'
         f'，再除以{取最簡約數(股數)}股之每股盈餘'
         )
    from twse_crawler.營收分析 import 取預測盈餘說明
    預估說明 = [f'{預估淨利結果.各項預估說明}'
               ,f'【每股盈餘預測結果】{取預測盈餘說明(年度每股盈餘, 前年至次年各季數據)}'
               ]
    預估說明 = '；'.join(預估說明)
    營收 = 取歷月營收表(股票)
    預測結果 = pd.Series({'前年至次年每股盈餘': pd.Series(年度每股盈餘)
                         ,'預測說明':預估說明
                         ,'預估說明':預估說明
                         ,'預估方法說明':預估方法說明
                         ,'最近財報季度':最近財報季度
                         ,'最近營收月份':營收.index.max()
                         })
    # 營收分析快取[f'以營收預測次年每股盈餘預({股票})'] = 預測結果
    return 預測結果

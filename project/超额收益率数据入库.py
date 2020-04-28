import os
import pandas
import datetime
import utils

c = utils.MySqlD(host='localhost', port=3306, database='gu',
                 user='root', passwd='root')

hs300 = pandas.read_csv('./data/gu_大盘_sh.000300_上证综指_data.csv', encoding='gbk')


# 计算 HPR 函数
def hpr(endPrice, periodPrice):
    # print(endPrice)
    endPrice = float(endPrice)
    periodPrice = float(periodPrice)
    return (endPrice - periodPrice) / periodPrice


# def run_chao_e_shou_yi(piao, name, data, end):
#     # cpi = ts.get_cpi()
#     # print(cpi)
#     print(piao)
#     # start = '2020-03-15'
#     # end = (datetime.datetime.fromisoformat(start)+datetime.timedelta(days=31)).isoformat().split('T')[0]
#     start = (datetime.datetime.fromisoformat(end)-datetime.timedelta(days=21)).isoformat().split('T')[0]
#     print(start, '=======>',end)
#     # piao = "002071"
#     # 获取沪深300历史数据
#
#
#     # print(dir(jl))
#     all_c = 0
#     last_jlr = None
#     # print(jl.to_json())
#     if len(jl) == 0:
#         return 0, name
#     for jlr in jl["date"]:
#         # print(jlr)
#         day_str =jlr
#         # print(day_str)
#         if last_jlr:
#             day_str_last = last_jlr
#
#             hpr_yearly = hpr(hs300[hs300["date"] == day_str]["close"], hs300[hs300["date"] == day_str_last]["close"])
#             jl_yearly = hpr(jl[jl["date"] == day_str]["close"], jl[jl["date"] == day_str_last]["close"])
#             # print('沪深300持有期收益率 HPR', hpr_yearly)
#             # print('金龙机电持有期收益率 HPR', jl_yearly)
#             # print('超额收益率', jl_yearly - hpr_yearly)
#             all_c += (jl_yearly - hpr_yearly)
#         last_jlr = jlr



for root, fs, fns in os.walk('./data'):
    for idx, fn in enumerate(fns):
        print(idx, fn)
        if not fn.endswith('.csv'):
            continue

        # import csv
        # with open(f'{root}{os.sep}{fn}', 'r', encoding='gbk') as f:
        #     print(f.readline())
        data = pandas.read_csv(f'{root}{os.sep}{fn}', encoding='gbk')
        keys = list(data.keys())
        res = []
        ar_d = {}
        for idx, line in enumerate(data.values.tolist()):
            _, industry, code_r, code_name, _ = fn.split('_')
            code_prefix, code = code_r.split('.')
            d = {k:v for k,v in zip(keys, line)}
            date = datetime.datetime.fromisoformat(line[0])
            dapan_ar = hs300[hs300["交易所行情日期"] == line[0]]
            if len(dapan_ar):
                ar = float(line[12]) - float(dapan_ar['涨跌'])
                ar_d[line[0]] = ar
            else:
                ar = 0

            d.update({
                'code': code,
                'name': code_name,
                'code_prefix': code_prefix,
                'time_date_int': date.timestamp(),
                'time_date_str': line[0],
                'time_date': line[0],
                'close': line[5],
                'industry': industry,
                'create_time': datetime.datetime.now(),
                'status': 0,
                'upp': line[12],
                'ar': ar,
                'car21': sum(ar_d.get(
                    (datetime.datetime.fromisoformat(line[0])-datetime.timedelta(days=i)).isoformat().split('T')[0], 0) for i in range(21)),
            })
            res.append(d)
            # print(d)

        # try:
        #     c.create_index('gu_test', res[0], pks='code,time_date_str')
        # except:
        #     pass
        c.save_data('gu_test', data=res)
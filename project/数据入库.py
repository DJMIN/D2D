import os
import pandas
import datetime
import utils

c = utils.MySqlD(host='localhost', port=3306, database='gu',
                 # user='root', passwd='root')
                 user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1')
for root, fs, fns in os.walk('./data'):
    for idx, fn in enumerate(fns):
        print(idx, fn)
        if not fn.endswith('.csv'):
            continue
        data = pandas.read_csv(f'{root}{os.sep}{fn}', encoding='gbk')
        keys = list(data.keys())
        res = []
        for line in data.values.tolist():
            _, industry, code_r, code_name, _ = fn.split('_')
            code_prefix, code = code_r.split('.')
            d = {k:v for k,v in zip(keys, line)}
            date = datetime.datetime.fromisoformat(line[0])
            d.update({
                'code': code,
                'code_prefix': code_prefix,
                'time_date_int': date.timestamp(),
                'time_date_str': line[0],
                'time_date': line[0],
                'close': line[5],
                'industry': industry,
                'create_time': datetime.datetime.now(),
                'status': 0,
                'upp': line[12]
            })
            res.append(d)

        # try:
        #     c.create_index('gu_test', res[0], pks='code,time_date_str')
        # except:
        #     pass
        c.save_data('gu1', data=res)

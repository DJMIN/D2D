# /usr/bin/env python
# coding: utf-8

import datetime
from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
from task import Migration
import re
import json

# 繁体转简体
# def cht_to_chs(line):
#     from langconv import Converter
#     line = Converter('zh-hans').convert(line)
#     # line.encode('utf-8')
#     return line



def main(_i):
    data = {}
    xlsxd = XlsxIbyFileD(path='./data').get_data(f'FI_T{_i}')
    res = []
    for line in xlsxd:
        if line.get("截止日期'", '').find('12-31') != -1:
            if line.get("截止日期'") not in data:
                data[line.get("截止日期'")] = {}
            if not line.get("股票代码'") in data[line.get("截止日期'")]:
                data[line.get("截止日期'")][line.get("股票代码'")] = line

    date_data = {}

    for date, v in data.items():
        count = {}
        num_key = {}
        # print(date)
        for gp, line in v.items():
            for key, value in line.items():
                if isinstance(value, float):
                    if key not in num_key:
                        num_key[key] = 0
                    num_key[key] += 1
                    if key not in count:
                        count[key] = 0
                    count[key] += value

        for ck, cv in count.items():
            # print(ck, cv/num_key[ck])
            if ck not in date_data:
                date_data[ck] = []
            if ck.find('率') != -1 and (ck.find('周转率') == -1):
                date_data[ck].append(f'{date.split("-")[0]}\t{cv/num_key[ck]*100:.2f}%')
            else:
                date_data[ck].append(f'{date.split("-")[0]}\t{cv/num_key[ck]:.2f}')

    for k, v in date_data.items():
        print(k)
        for d in v:
            print(d)


if __name__ == '__main__':
    for i in [
        # '1',
        # '2',
        # '4',
        # '5',
        '8',
    ]:
        main(i)

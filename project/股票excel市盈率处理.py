import logging
import time

import xlrd
import os
import shutil
import datetime
import pandas
import requests
import xlsxwriter
import zipfile
from os.path import join, getsize


def unzip_file(zip_src, dst_dir):
    r = zipfile.is_zipfile(zip_src)
    if r:
        fz = zipfile.ZipFile(zip_src, 'r')
        if not os.path.exists(dst_dir):
            os.makedirs(dst_dir)
        for file in fz.namelist():
            fz.extract(file, dst_dir)
    else:
        print('This is not zip')
        raise ValueError('This is not zip')

def download(date):
    path = './data/' + date+'.zip'
    print(path)
    if os.path.exists(path):
        return
    retry = 0
    while True:
        try:
            with open(path, 'wb') as f:
                content = requests.get(f'http://47.97.204.47/syl/{date}.zip').content
                if content == b'Not found.':
                    break
                f.write(content)
            unzip_file(path, 'datazip')
            break
        except ValueError:
            retry+=1
            print(retry)
            time.sleep(1)
            if retry > 3:
                print(requests.get(f'http://47.97.204.47/syl/{date}.zip').content)


def get_date(date, plus):
    return (datetime.datetime.fromisoformat(date) + datetime.timedelta(days=plus)).isoformat().split('T')[0]


def get_data(path):
    workbook = xlrd.open_workbook(path,encoding_override="cp1252")  # 文件路径
    # 获取所有sheet的名字
    all_res = {}
    for idx, name in enumerate(workbook.sheet_names()):
        logging.info(f'sheet:{idx}:{name}')
        worksheet = workbook.sheet_by_index(idx)
        nrows = worksheet.nrows
        keys = {i: key.lower().replace('\n', '') for i, key in enumerate(worksheet.row_values(0))}
        res = []
        for line_num in range(1, nrows):
            line = worksheet.row_values(line_num)
            data = {keys[i]: key for i, key in enumerate(line)}
            res.append(data)
        all_res[name] = res
    return all_res


def get_pandas_row_value(data, key):
    return list(data.to_dict()[key].values())[0]


def dictlist_to_pandas(data):
    return pandas.DataFrame(data, columns=list(data[0].keys()))


def get_today_str():
    return datetime.date.today().isoformat()


def download_main():
    for date in [
        get_date('2014-10-14', -i).replace('-', '') for i in range(2000)
    ]:
        download(date)

def main():
    piao = '300032'
    result_list = []
    for date in [
        '2014-06-30',
        '2014-07-03',
        '2014-08-27',
        '2014-08-29',
        '2014-09-01',
        '2014-09-22',
        '2014-09-25',
        '2015-02-11',
        '2015-02-12',
    ]:
        res = get_data("datazip/" + date.replace('-', '') + '.xls')
        gegu_all = dictlist_to_pandas(res['个股数据'])
        gegu = gegu_all[gegu_all['证券代码'] == piao]
        gegu_d = get_pandas_row_value(gegu, '证监会大类名称')

        hang_jin_yin_all = dictlist_to_pandas(res['证监会行业静态市盈率'])
        hang_jin_yin = hang_jin_yin_all[hang_jin_yin_all['行业名称'] == gegu_d]
        hang_dong_yin_all = dictlist_to_pandas(res['证监会行业滚动市盈率'])
        hang_dong_yin = hang_dong_yin_all[hang_dong_yin_all['行业名称'] == gegu_d]
        hang_jin_all = dictlist_to_pandas(res['证监会行业市净率'])
        hang_jin = hang_jin_all[hang_jin_all['行业名称'] == gegu_d]

        result_list.append({
            "日期": date,
            '个股静态市盈率': get_pandas_row_value(gegu, '个股静态市盈率'),
            '行业静态市盈率': get_pandas_row_value(hang_jin_yin, '最新静态市盈率'),
            '个股滚动市盈率': get_pandas_row_value(gegu, '个股滚动市盈率'),
            '行业滚动市盈率': get_pandas_row_value(hang_dong_yin, '最新滚动市盈率'),
            '个股市净率': get_pandas_row_value(gegu, '个股市净率'),
            '行业市净率': get_pandas_row_value(hang_jin, '最新市净率'),
        })
    dictlist_to_pandas(result_list).to_excel('output1.xlsx', engine='xlsxwriter')


if __name__ == '__main__':
    download_main()
    main()
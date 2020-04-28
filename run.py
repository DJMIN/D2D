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


def format_phone(phone):
    if isinstance(phone, float):
        phone = str(int(phone))
    return phone.strip()


def format_str(string):
    if isinstance(string, float):
        string = str(int(string))
    return string.strip()


def format_number(number):
    if isinstance(number, float) and number % 1 == 0.0:
        number = int(number)
    return number


def add_code(d):
    if 'Code' in d:
        return d['Code']
    phone = d['MobileNoTemp']
    if (
            phone.startswith('(') or
            phone.startswith('（') or
            (len(phone) == 12 and phone.startswith('085'))
    ):
        code = phone[1:4]
    elif (
            (len(phone) == 12 and phone[4] == '-') or
            (len(phone) == 11 and phone.startswith('85'))
    ):
        code = phone[0:3]
    elif (
            (len(phone) == 12 and phone.startswith('0085'))
    ):
        code = phone[2:5]
    else:
        raise ValueError(phone)
    return code


def add_type(d):
    if 'Type' in d:
        return d['Type']
    f_phone = add_phone(d)
    if f_phone[0] in ('2', '3', '4'):
        phone_type = 0
    elif f_phone[0] in ('5', '6', '7', '8', '9'):
        phone_type = 1
    else:
        raise ValueError(d)
    return phone_type


def add_phone(d):
    phone = d['MobileNoTemp']
    phone = re.findall(r'(.*)?(\d{3})?[-\s)）]*?(\d{8})[^\d]+', phone+'#')
    if phone:
        phone = phone[0][2]
    else:
        raise ValueError(phone,  d['MobileNoTemp'])

    return phone


PID = MySqlD(host='localhost', port=4002, database='727ssnew', user='root', passwd='1234qwer!@#$QWER')._execute(
    "select Pid from people order by Pid desc limit 1"
)[1].__next__()['Pid']


def add_pid(_):
    global PID
    PID += 1
    return PID


def format_data_people(data):
    """
    这里可以重写“修改table行数据再迁移到新table”函数
    :param data: dict table的行数据字典
    :return: dict 修改后table的行数据字典
    """
    new_data = {}
    change = {
        'userid': '用户ID',
        'Pid': 'Pid',
        'c_name': 'CName',
        '中文名': 'CName',
        'e_name': 'EngName',
        '英文名': 'EngName',
        '性别': 'Sex',
        '出生日': 'Birth',
        '证件号': 'IDNo',
        '通行证': 'HKTon',
        '手机号': 'MobileNoTemp',
        '证据': 'Evidence',
        '来源': 'Source',
        'code': 'Code',
        'pid': 'Pid',
        'gender': 'Sex',
        'birth': 'Birth',
        'hongkong_id': 'IDNo',
        # 'mainland_id': 'IDNo',
        'phonenumber': 'MobileNoTemp',
        '备注': 'Memo',
    }
    new_key_format_func = {
        'MobileNoTemp': format_phone,
        'Code': format_str,
        'Pid': format_str,
    }
    add_prams = {
        'Code': add_code,
        'Type': add_type,
        'MobileNo': add_phone,
        'Pid': add_pid,
    }
    remove_prams = {
        'MobileNoTemp': None,
    }
    for old_key, new_key in change.items():
        if old_key in data:
            if new_key in new_data:
                raise ValueError(f'{old_key} -> {new_key} {new_key}重复 [{new_data[new_key]}] --覆盖> [{data[old_key]}]')
            new_data[new_key] = new_key_format_func.get(new_key, lambda r: r)(data[old_key])
    for add_key, add_key_func in add_prams.items():
        new_data[add_key] = add_key_func(new_data)
    for remove_key, remove_func in remove_prams.items():
        if remove_key in new_data:
            new_data.pop(remove_key)
    return new_data


def format_data_pccw_called(data):
    new_data = {
        'Callee': data['Callee'],
        'Caller': data['Caller'],
    }
    return new_data


def test1():
    esd = ElasticSearchD(hosts='127.0.0.1:9284')
    tidbd = MySqlD(host='localhost', port=4002, database='727ssnew', user='root', passwd='1234qwer!@#$QWER')
    csvd = CsvD(path='./data')
    jsond = JsonListD(path='./data')
    xlsxd = XlsxIbyFileD(path='./data')
    task = Migration(
        database_from=esd,
        database_to=xlsxd,
        # database_to=tidbd,
        table_from=('pccw_called_*', {
            "query": {
                "terms": {
                    "Caller": [
                        "91241940",
                        "96846890",
                        "90860057",
                        "60807273",
                        "63450498",
                    ]
                }
            }
        }),
        table_to='pccw22',
        # count_from=8429072,
        # size=10000,
    )
    task.format_data = format_data_pccw_called
    task.run()


def test2():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        table_from='user',
        table_to='user1'
    )
    t.run()


def test3():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=ElasticSearchD(hosts='127.0.0.1:9200'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test4():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=CsvD(path='./data'),
        table_from='user1',
        table_to='user1'
    )
    t.run()


def test5():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=CsvD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    t.run()


def test6():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=JsonListD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    t.run()


def test7():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=XlsIbyFileD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    t.run()


def test8():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=XlsxIbyFileD(path='./data1'),
        table_from='user1',
        table_to='user2'
    )
    t.run()


def test9():
    t = Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:19200'),
        database_to=CsvD(path='./data1'),
        table_from='phone',
        table_to='phone'
    )
    t.run()


def test10():
    t = Migration(
        database_from=CsvD(path='./data1'),
        database_to=XlsxIbyFileD(path='./data1'),
        table_from='phone',
        table_to='phone'
    )
    t.run()


def test11():
    task = Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:19200'),
        database_to=XlsxIbyFileD(path='./data1'),
        table_from='phone',
        table_to='phone'
    )
    task.run()


def test12():
    task = Migration(
        database_from=CsvD(path='./data', encoding='utf8'),
        database_to=CsvD(path='./data1', encoding='utf8'),
        table_from='user',
        table_to=f"user_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_bak"
    )

    def format_data(data):
        """
        这里可以重写“修改table行数据再迁移到新table”函数
        :param data: dict table的行数据字典
        :return: dict 修改后table的行数据字典
        """
        new_data = {}
        for key in data.keys():
            if key == 'userid':
                new_data['用户ID'] = data.get('userid')
            elif key == 'phonenumber':
                new_data['手机号码'] = data.get('phonenumber')
        return new_data

    task.format_data = format_data

    task.run()


def test13():
    task = Migration(
        database_from=CsvD(path='./data', encoding='utf8'),
        database_to=MongoDBD(hosts="mongodb://localhost:27017/", database='test'),
        table_from='user',
        table_to=f"user"
    )
    task.run()


def test14():
    task = Migration(
        database_from=MongoDBD(hosts="mongodb://localhost:27017/", database='test'),
        database_to=MongoDBD(hosts="mongodb://localhost:27017/", database='test'),
        table_from='user',
        table_to=f"user1"
    )
    task.run()


def test15():
    task = Migration(
        database_from=MongoDBD(hosts="mongodb://localhost:27017/", database='test'),
        database_to=CsvD(path='./data', encoding='utf8'),
        table_from='user',
        table_to=f"user1"
    )
    task.run()


def test16():
    task = Migration(
        database_from=CsvD(path='./data', encoding='utf8'),
        database_to=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        table_from='user',
        table_to=f"user_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_bak",
        pks='用户ID,手机号码'
    )

    def format_data(data):
        """
        这里可以重写“修改table行数据再迁移到新table”函数
        :param data: dict table的行数据字典
        :return: dict 修改后table的行数据字典
        """
        new_data = {}
        for key in data.keys():
            if key == 'userid':
                new_data['用户ID'] = data.get('userid')
            elif key == 'phonenumber':
                new_data['手机号码'] = data.get('phonenumber')
        return new_data

    task.format_data = format_data

    task.run()


if __name__ == '__main__':
    # test12()  # test12是从csv到csv不需要网络配置可以直接尝试运行
    test1()

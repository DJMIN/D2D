# /usr/bin/env python
# coding: utf-8

import datetime
from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD
from task import Migration


def test1():
    t = Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:19200'),
        database_to=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
    )
    t.run()


def test2():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        data_from='user',
        data_to='user1'
    )
    t.run()


def test3():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=ElasticSearchD(hosts='127.0.0.1:9200'),
        data_from='user1',
        data_to='user1'
    )
    t.run()


def test4():
    t = Migration(
        database_from=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_to=CsvD(path='./data'),
        data_from='user1',
        data_to='user1'
    )
    t.run()


def test5():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=CsvD(path='./data1'),
        data_from='user1',
        data_to='user2'
    )
    t.run()


def test6():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=JsonListD(path='./data1'),
        data_from='user1',
        data_to='user2'
    )
    t.run()


def test7():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=XlsIbyFileD(path='./data1'),
        data_from='user1',
        data_to='user2'
    )
    t.run()


def test8():
    t = Migration(
        database_from=CsvD(path='./data'),
        database_to=XlsxIbyFileD(path='./data1'),
        data_from='user1',
        data_to='user2'
    )
    t.run()


def test9():
    t = Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:19200'),
        database_to=CsvD(path='./data1'),
        data_from='phone',
        data_to='phone'
    )
    t.run()


def test10():
    t = Migration(
        database_from=CsvD(path='./data1'),
        database_to=XlsxIbyFileD(path='./data1'),
        data_from='phone',
        data_to='phone'
    )
    t.run()


def test11():
    task = Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:19200'),
        database_to=XlsxIbyFileD(path='./data1'),
        data_from='phone',
        data_to='phone'
    )
    task.run()


def test12():
    task = Migration(
        database_from=CsvD(path='./data', encoding='utf8'),
        database_to=CsvD(path='./data1', encoding='utf8'),
        data_from='user',
        data_to=f"user_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}_bak"
    )
    task.run()


if __name__ == '__main__':
    test12()

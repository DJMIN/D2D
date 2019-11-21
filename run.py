# /usr/bin/env python
# coding: utf-8

import datetime
from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
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


if __name__ == '__main__':
    test12()  # test12是从csv到csv不需要网络配置可以直接尝试运行

# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils.db
from utils.db import ElasticSearchD, MySqlD
import time
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
        database_to=MySqlD(host='localhost', port=3306, database='test',
                           user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        database_from=MySqlD(host='localhost', port=3306, database='test',
                             user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        data_from='tguser',
        data_to='tguser1'
    )
    t.run()


if __name__ == '__main__':
    test1()
    test2()

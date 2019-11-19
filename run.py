# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils.db
from utils.db import ElasticSearchD, MySqlD
import time
from task import Migration

if __name__ == '__main__':
    Migration(
        database_from=ElasticSearchD(hosts='127.0.0.1:9200'),
        database_to=MySqlD(
            host='localhost', port=3306, database='test', user='debian-sys-maint', passwd='BjOtjlf6bDqypoH1'),
        data_from={"query": {}, "index": 'user'},
        data_to={"tablename": 'user'}
    )

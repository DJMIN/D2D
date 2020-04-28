# /usr/bin/env python
# coding: utf-8


import datetime
from utils.db import ElasticSearchD, MySqlD, CsvD, JsonListD, XlsIbyFileD, XlsxIbyFileD, MongoDBD
from task import Migration2DB
import re


def main():
    # esd = ElasticSearchD(hosts='127.0.0.1:9284')
    # tidbd = MySqlD(host='localhost', port=4002, database='727ssnew', user='root', passwd='1234qwer!@#$QWER')
    # csvd = CsvD(path='./data')
    # jsond = JsonListD(path='./data')
    task = Migration2DB(
        database_from1=XlsxIbyFileD(path='./data'),
        database_from2=XlsxIbyFileD(path='./data'),
        table_from1=f'''people''',
        table_from2=f'''pccw11''',
        migration_key1=f'''MobileNo''',
        migration_key2=f'''Callee''',
        database_to=XlsxIbyFileD(path='./data'),
        table_to='peoplezd123'
                 '',
        # size=10000,
    )
    # task.format_data = format_data_people
    task.run()


if __name__ == '__main__':
    main()

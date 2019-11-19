#!/usr/bin/env python3
# coding: utf-8


import os
import random
from abc import ABC

from .myutils import EsModel
from .myutils import BaseDB
from .myutils import ExcelWriter


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


class ElasticSearchD(EsModel):
    def __init__(self, hosts):
        super().__init__(hosts)

    def get_data(self, *args, **kwargs):
        for i in self.scan(*args, **kwargs):
            r = i['_source']
            if r.get('id'):
                r['_id'] = i['_id']
            else:
                r['id'] = i['_id']
            yield r

    def save_data(self, index, data, batch_size=1000, uuid_key=('id',), *args, **kwargs):
        actions = []
        for d in data:
            _id = '-'.join(d[k] for k in uuid_key)
            actions.append({
                '_op_type': 'index',  # 操作 index update create delete
                '_index': index,  # index
                '_type': '_doc',  # type
                "_id": f"{_id}",
                '_source': d})
            if len(actions) > 2000:
                self.bulk_write(actions, *args, **kwargs)
                actions = []

        if len(actions):
            self.bulk_write(actions, *args, **kwargs)


class MySqlD(BaseDB):
    def __init__(self, host, port, user, passwd, database=None):
        super().__init__(host, port, database, user, passwd)

    def get_data(self, *args, **kwargs):
        self.__execute__(*args, **kwargs)

    def save_data(self, *args, **kwargs):
        self.insert_many_with_dict_list(*args, **kwargs)

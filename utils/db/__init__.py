#!/usr/bin/env python3
# coding: utf-8


import os
import random
import logging
from abc import ABC

from .myutils import EsModel
from .myutils import BaseDB
from .myutils import ClientPyMySQL
from .myutils import ExcelWriter


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


class ElasticSearchD(EsModel):
    def __init__(self, hosts):
        super().__init__(hosts)

    def get_data(self, index, *args, **kwargs):
        for i in self.scan(query={}, index=index, *args, **kwargs):
            r = {}
            if r.get('id'):
                r['_id'] = i['_id']
            else:
                r['id'] = i['_id']
            r.update(i['_source'])
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
            if len(actions) > batch_size:
                self.bulk_write(actions, *args, **kwargs)
                actions = []

        if len(actions):
            self.bulk_write(actions, *args, **kwargs)

    def get_indexes(self):
        return list(self.es.indices.get_alias().keys())

    def get_count(self, index):
        return int(self.es.count(index=index, body={})['count'])

    @staticmethod
    def get_int_type_from_len(length):
        if 0 < length <= 8:
            return "byte"
        elif 8 < length <= 16:
            return "short"
        elif 16 < length <= 32:
            return "integer"
        elif 32 < length <= 64:
            return "long"
        return None

    @staticmethod
    def get_str_type_from_len(length):
        if 0 < length <= 256:
            return "keyword"
        elif 256 < length <= 65536:
            return "text"
        elif 65536 < length <= 16777216:
            return "text"
        elif 16777216 < length <= 4294967295:
            return "text"
        return None

    def create_index(self, index, data, pks='id'):
        # sql = """SET NAMES utf8mb4;"""
        # sql += """SET FOREIGN_KEY_CHECKS = 0;"""
        # if drop == 1:
        #     sql += """DROP TABLE IF EXISTS `{}`;""".format(tbname)
        pairs = {
            "settings": {
                "index": {
                    "number_of_shards": 12,
                    "number_of_replicas": 1
                }
            },
            'mappings': {
                'properties': {

                }
            }
        }
        properties = pairs['mappings']['properties']

        for name, value in data.items():
            properties[name] = {}
            if isinstance(value, int):
                properties[name]['type'] = 'long'
            elif isinstance(value, str):
                properties[name]['type'] = {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 1024
                        }
                    },
                    "analyzer": "ik_max_word",
                    "search_analyzer": "ik_smart"
                }
        if self.es.indices.exists(index=index) is not True:
            res = self.es.indices.create(index=index, body=pairs)
            if not res:
                logging.error("错误，创建索引{}失败".format(index))
                return False

            logging.info("正常，创建索引{}成功".format(index))
            return True
        else:
            logging.info("正常，索引{}已存在".format(index))
            return True


# class MySqlD(BaseDB, ABC):
#     def __init__(self, host, port, user, passwd, database=None):
#         super().__init__(host, port, database, user, passwd)
#
#     def get_data(self, index, *args, **kwargs):
#         return self.__execute__(sql=f'select * from {index}', *args, **kwargs).fetchall()
#
#     def save_data(self, index, *args, **kwargs):
#         self.insert_many_with_dict_list(tablename=index, *args, **kwargs)
#
#     def get_indexes(self):
#         tbs = []
#         for r in list(self.__execute__(sql='show tables;')):
#             tbs.append(r[0])
#         return tbs
#
#     def get_count(self, index):
#         return int(self.__execute__(sql=f'select count(0) as c from {index}').fetchone()[0])
#
#     @staticmethod
#     def get_int_type_from_len(length):
#         if 0 < length <= 8:
#             return "tinyint"
#         elif 8 < length <= 16:
#             return "smallint"
#         elif 16 < length <= 32:
#             return "int"
#         elif 32 < length <= 64:
#             return "bigint"
#         return None
#
#     @staticmethod
#     def get_str_type_from_len(length):
#         if 0 < length <= 256:
#             return "varchar({})".format(length)
#         elif 256 < length <= 65536:
#             return "text"
#         elif 65536 < length <= 16777216:
#             return "mediumtext"
#         elif 16777216 < length <= 4294967295:
#             return "longtext"
#         return None
#
#     def create_index(self, index, data, pks='id'):
#         # sql = """SET NAMES utf8mb4;"""
#         # sql += """SET FOREIGN_KEY_CHECKS = 0;"""
#         # if drop == 1:
#         #     sql += """DROP TABLE IF EXISTS `{}`;""".format(tbname)
#         sql = """create table if not exists {}(""".format(index)
#         for name, value in data.items():
#             if isinstance(value, int):
#                 _type = 'bigint'
#             elif isinstance(value, str):
#                 if pks.find(name) > -1:
#                     _type = 'varchar(256)'
#                 else:
#                     _type = 'text'
#             else:
#                 _type = 'blob'
#
#             sql += ('`' + name + '` ')
#             sql += (_type + ',')
#
#         slen = len(pks)
#         if slen > 0:
#             sql += """PRIMARY KEY ("""
#             v_keys = pks.split(',')
#             first = 1
#             for key in v_keys:
#                 if first == 1:
#                     first = 0
#                     sql += """`{}`""".format(key)
#                 else:
#                     sql += """,`{}`""".format(key)
#             sql += """)) """
#         else:
#             sql = sql[0: -1]
#             sql += ')'
#
#         sql += """ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
#         print(sql)
#         self.__execute__(sql)
#         self.__commit__()


class MySqlD(ClientPyMySQL, ABC):
    def __init__(self, host, port, user, passwd, database=None):
        super().__init__(host, port, database, user, passwd)

    def get_data(self, index, *args, **kwargs):
        return self._execute(sql=f'select * from {index}', *args, **kwargs)[1]

    def save_data(self, index, *args, **kwargs):
        self.insert_many_with_dict_list(tablename=index, *args, **kwargs)

    def get_indexes(self):
        return list(t['Tables_in_test'] for t in self._execute(sql='show tables;')[1])

    def get_count(self, index):
        return self._execute(f'select count(0) as c from  {index}', )[1][0]['c']

    @staticmethod
    def get_int_type_from_len(length):
        if 0 < length <= 8:
            return "tinyint"
        elif 8 < length <= 16:
            return "smallint"
        elif 16 < length <= 32:
            return "int"
        elif 32 < length <= 64:
            return "bigint"
        return None

    @staticmethod
    def get_str_type_from_len(length):
        if 0 < length <= 256:
            return "varchar({})".format(length)
        elif 256 < length <= 65536:
            return "text"
        elif 65536 < length <= 16777216:
            return "mediumtext"
        elif 16777216 < length <= 4294967295:
            return "longtext"
        return None

    def create_index(self, index, data, pks='id'):
        # sql = """SET NAMES utf8mb4;"""
        # sql += """SET FOREIGN_KEY_CHECKS = 0;"""
        # if drop == 1:
        #     sql += """DROP TABLE IF EXISTS `{}`;""".format(tbname)
        sql = """create table if not exists {}(""".format(index)
        for name, value in data.items():
            if isinstance(value, int):
                _type = 'bigint'
            elif isinstance(value, str):
                if pks.find(name) > -1:
                    _type = 'varchar(256)'
                else:
                    _type = 'text'
            else:
                _type = 'blob'

            sql += ('`' + name + '` ')
            sql += (_type + ',')

        slen = len(pks)
        if slen > 0:
            sql += """PRIMARY KEY ("""
            v_keys = pks.split(',')
            first = 1
            for key in v_keys:
                if first == 1:
                    first = 0
                    sql += """`{}`""".format(key)
                else:
                    sql += """,`{}`""".format(key)
            sql += """)) """
        else:
            sql = sql[0: -1]
            sql += ')'

        sql += """ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci ROW_FORMAT = Dynamic;"""
        print(sql)
        self._execute(sql)
        self.end_transaction()


# /usr/bin/env python
# coding: utf-8

import os
import sys
import json
import utils
import time
import datetime
import logging


def format_value(data):
    if isinstance(data, float) and data % 1 == 0.0:
        data = int(data)
    elif isinstance(data, str):
        data = data.strip()
    return data


class Migration(object):
    def __init__(
            self, database_from, database_to, table_from=None, table_to=None,
            pks='id', pkd=None, windows=1000, count_from=None, size=None, quchong=False):
        self.database_from = database_from
        self.database_to = database_to
        self.table_from = table_from
        self.table_to = table_to
        self.count_from = count_from
        self.size = size
        self.pks = pks
        self.pkd = pkd or {}
        self.windows = windows
        self.quchong = quchong
        self.all_new_data_json_string = set({})

    def run(self):
        if self.table_from:
            table_to = self.table_to or self.table_from
            table_from = self.table_from
            self.run_one(table_from, table_to, self.pks)
        else:
            for table_from_raw in self.database_from.get_indexes():
                # table_from = table_from_raw
                # table_to = table_from_raw
                # if isinstance(self.database_from, utils.ElasticSearchD):
                #     table_from = {'index':table_from_raw}
                #
                # elif isinstance(self.database_from, utils.MySqlD):
                #     table_from = {'index':table_from_raw}
                #
                # if isinstance(self.database_to, utils.ElasticSearchD):
                #     table_to = {'index':table_from_raw}
                #
                # elif isinstance(self.database_to, utils.MySqlD):
                #     table_to = {'index':table_from_raw}

                self.run_one(table_from_raw, table_from_raw, self.pkd.get(table_from_raw, 'id'))

    def run_one(self, table_from, table_to, pks):
        count = self.count_from or self.database_from.get_count(table_from)
        table = self.database_from.get_data(table_from)
        action = []
        time_start = time.time()
        for idx, d in enumerate(table):
            try:
                f_d = self.format_data(d)
                if self.quchong:
                    if json.dumps(f_d) in self.all_new_data_json_string:
                        continue
                    else:
                        self.all_new_data_json_string.add(json.dumps(f_d))
            except Exception as e:
                logging.info(f'{self.database_from}/{table_from}:{idx} {e}')
                raise e
            action.append(f_d)
            if idx == 0:
                self.database_to.create_index(index=table_to, data=f_d, pks=pks)
            if (idx < 5) or (not idx % 1000):
                # for k1,  k2, in zip(d.items(), f_d.items()):
                #     utils.g_log.info('{} -> {} | {}==>{}'.format(
                #
                #     ))
                utils.g_log.info('{}\n|{}\n|\n|{}\n|{}\n'.format(f'{idx:080d}', d, f_d, f'{idx:0163d}'))

            if (self.size is not None) and (self.size < idx):
                break
            if len(action) > self.windows:
                time_use = time.time() - time_start
                proc = (idx + 1) / count

                utils.g_log.info('[{:012d}/{:012d}] {:0>8s}/{:0>8s}  ...{:.2f}%   {}/{} -> {}/{}'.format(
                    # self.database_to.get_count(table_to),
                    idx + 1, count, datetime.timedelta(seconds=time_use).__str__().split('.')[0],
                    datetime.timedelta(seconds=time_use / proc).__str__().split('.')[0],
                    proc * 100,
                    self.database_from, table_from,
                    self.database_to, table_to,
                    ))
                utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': table_to})
                action = []
        if len(action):
            utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': table_to})
        # action = []

    @staticmethod
    def format_data(data):
        """
        修改table行数据再迁移到新的table

        :param data: dict table的行数据字典
        :return: dict 修改后table的行数据字典
        """
        return data


class Migration2DB(object):
    def __init__(
            self, database_from1, database_from2, database_to, table_from1, table_from2, table_to,
            migration_key1, migration_key2, pks='id', pkd=None, windows=1000,
            count_from1=None, count_from2=None, size=None, quchong=False):
        self.database_from1 = database_from1
        self.database_from2 = database_from2
        self.database_to = database_to
        self.table_from1 = table_from1
        self.table_from2 = table_from2
        self.table_to = table_to
        self.migration_key1 = migration_key1
        self.migration_key2 = migration_key2 or migration_key1
        self.count_from1 = count_from1
        self.count_from2 = count_from2
        self.size = size
        self.pks = pks
        self.pkd = pkd or {}
        self.windows = windows
        self.quchong = quchong
        self.all_new_data_json_string = set({})

    def run(self):
        if self.table_from1:
            table_to = self.table_to or self.table_from1
            table_from = self.table_from1
            self.run_one(table_from, table_to, self.pks)
        else:
            for table_from_raw in self.database_from1.get_indexes():
                # table_from = table_from_raw
                # table_to = table_from_raw
                # if isinstance(self.database_from, utils.ElasticSearchD):
                #     table_from = {'index':table_from_raw}
                #
                # elif isinstance(self.database_from, utils.MySqlD):
                #     table_from = {'index':table_from_raw}
                #
                # if isinstance(self.database_to, utils.ElasticSearchD):
                #     table_to = {'index':table_from_raw}
                #
                # elif isinstance(self.database_to, utils.MySqlD):
                #     table_to = {'index':table_from_raw}

                self.run_one(table_from_raw, table_from_raw, self.pkd.get(table_from_raw, 'id'))

    def run_one(self, table_from1, table_to, pks):
        count1 = self.count_from1 or self.database_from1.get_count(table_from1)
        count2 = self.count_from2 or self.database_from1.get_count(table_from1)
        table = self.database_from1.get_data(table_from1)
        action = []
        time_start = time.time()
        table2d = {}
        for idx, d in enumerate(self.database_from2.get_data(self.table_from2)):
            if (idx < 5) or (not idx % 1000):
                utils.g_log.info('{}\n|{}'.format(f'{idx:080d}', d))
                time_use = time.time() - time_start
                proc = (idx + 1) / count2

                utils.g_log.info('[{:012d}/{:012d}] {:0>8s}/{:0>8s}  ...{:.2f}%   {}/{} Loading'.format(
                    idx + 1, count2, datetime.timedelta(seconds=time_use).__str__().split('.')[0],
                    datetime.timedelta(seconds=time_use / proc).__str__().split('.')[0],
                    proc * 100,
                    self.database_from2, self.table_from2,
                ))

            table2d[format_value(d[self.migration_key2]).__str__()] = {k: format(v) for k, v in d.items()}

        for idx, d in enumerate(table):
            try:
                f_d = self.format_data(d, table2d.get(format_value(d.get(self.migration_key1, '')).__str__(), {}))

                if self.quchong:
                    if json.dumps(f_d) in self.all_new_data_json_string:
                        continue
                    else:
                        self.all_new_data_json_string.add(json.dumps(f_d))
            except Exception as e:
                logging.info(f'{self.database_from1}/{table_from1}:{idx} {e}')
                raise e
            action.append(f_d)
            if idx == 0:
                temp_d = {}
                for k, v in f_d.items():
                    temp_d[k] = v
                if table2d:
                    for k, v in table2d[list(table2d.keys())[0]].items():
                        temp_d[k] = v
                self.database_to.create_index(index=table_to, data=temp_d, pks=pks)
            if (idx < 5) or (not idx % 1000):
                # for k1,  k2, in zip(d.items(), f_d.items()):
                #     utils.g_log.info('{} -> {} | {}==>{}'.format(
                #
                #     ))
                utils.g_log.info('{}\n|{}\n|\n|{}\n|{}\n'.format(f'{idx:080d}', d, f_d, f'{idx:0163d}'))

            if (self.size is not None) and (self.size < idx):
                break
            if len(action) > self.windows:
                time_use = time.time() - time_start
                proc = (idx + 1) / count1

                utils.g_log.info('[{:012d}/{:012d}] {:0>8s}/{:0>8s}  ...{:.2f}%   {}/{} -> {}/{}'.format(
                    # self.database_to.get_count(table_to),
                    idx + 1, count1, datetime.timedelta(seconds=time_use).__str__().split('.')[0],
                    datetime.timedelta(seconds=time_use / proc).__str__().split('.')[0],
                    proc * 100,
                    self.database_from1, table_from1,
                    self.database_to, table_to,
                    ))
                utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': table_to})
                action = []
        if len(action):
            utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': table_to})
        # action = []

    @staticmethod
    def format_data(data1, data2):
        new_data = data1
        for k, v in data2.items():
            new_data[k] = v

        return new_data

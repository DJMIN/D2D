# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils
import time
import datetime


class Migration(object):
    def __init__(self, database_from, database_to, table_from=None, table_to=None, pks='id', pkd=None, windows=1000):
        self.database_from = database_from
        self.database_to = database_to
        self.table_from = table_from
        self.table_to = table_to
        self.pks = pks
        self.pkd = pkd or {}
        self.windows = windows

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
        table = self.database_from.get_data(table_from)
        count = self.database_from.get_count(table_from)
        action = []
        time_start = time.time()
        for idx, d in enumerate(table):
            f_d = self.format_data(d)
            action.append(f_d)
            if idx == 0:
                self.database_to.create_index(index=table_to, data=f_d, pks=pks)
            if not (idx + 1) % self.windows:
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

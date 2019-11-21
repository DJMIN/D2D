# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils
import time
import datetime


class Migration(object):
    def __init__(self, database_from, database_to, table_from=None, table_to=None, windows=1000):
        self.database_from = database_from
        self.database_to = database_to
        self.table_from = table_from
        self.table_to = table_to
        self.windows = windows

    def run(self):
        if self.table_from:
            table_to = self.table_to or self.table_from
            table_from = self.table_from
            self.run_one(table_from, table_to)
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

                self.run_one(table_from_raw, table_from_raw)

    def run_one(self, table_from, table_to):
        data = self.database_from.get_data(table_from)
        count = self.database_from.get_count(table_from)
        action = []
        time_start = time.time()
        for idx, d in enumerate(data):
            if idx == 0:
                self.database_to.create_index(index=table_to, data=d)
            action.append(self.format_data(d))
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
    def format_data(d):
        return d

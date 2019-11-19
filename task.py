# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils
import time
import datetime


class Migration(object):
    def __init__(self, database_from, database_to, data_from=None, data_to=None, windows=1000):
        self.database_from = database_from
        self.database_to = database_to
        self.data_from = data_from
        self.data_to = data_to
        self.windows = windows

    def run(self):
        if self.data_from:
            data_to = self.data_to or self.data_from
            data_from = self.data_from
            self.run_one(data_from, data_to)
        else:
            for data_from_raw in self.database_from.get_indexes():
                # data_from = data_from_raw
                # data_to = data_from_raw
                # if isinstance(self.database_from, utils.ElasticSearchD):
                #     data_from = {'index':data_from_raw}
                #
                # elif isinstance(self.database_from, utils.MySqlD):
                #     data_from = {'index':data_from_raw}
                #
                # if isinstance(self.database_to, utils.ElasticSearchD):
                #     data_to = {'index':data_from_raw}
                #
                # elif isinstance(self.database_to, utils.MySqlD):
                #     data_to = {'index':data_from_raw}

                self.run_one(data_from_raw, data_from_raw)

    def run_one(self, data_from, data_to):
        data = self.database_from.get_data(data_from)
        count = self.database_from.get_count(data_from)
        action = []
        time_start = time.time()
        for idx, d in enumerate(data):
            if idx == 0:
                self.database_to.create_index(index=data_to, data=d)
            action.append(self.format_data(d))
            if not (idx + 1) % self.windows:
                time_use = time.time() - time_start
                proc = (idx + 1) / count

                utils.g_log.info('[{:012d}/{:012d}/{:012d}] {:8s}/{:8s}  ...{:.2f}%   {}:{} -> {}:{}'.format(
                    self.database_to.get_count(data_to),
                    idx + 1, count, datetime.timedelta(seconds=time_use).__str__().split('.')[0],
                    datetime.timedelta(seconds=time_use / proc).__str__().split('.')[0],
                    proc * 100,
                    self.database_from.__class__.__name__[:-1], data_from,
                    self.database_to.__class__.__name__[:-1], data_to,
                    ))
                utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': data_to})
                action = []
        if len(action):
            utils.run_task_auto_retry(self.database_to.save_data, kwargs={"data": action, 'index': data_to})
        # action = []

    @staticmethod
    def format_data(d):
        return d

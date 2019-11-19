# /usr/bin/env python
# coding: utf-8

import os
import sys
import pymysql
import utils
import time


class Migration(object):
    def __init__(self, database_from, database_to, data_from, data_to):
        self.database_from = database_from
        self.database_to = database_to
        self.data_from = data_from
        self.data_to = data_to

    def run(self):
        esc = self.database_from
        msc = self.database_to
        data = esc.get_data(**self.data_from)
        action = []
        for d in data:
            action.append(d)
            if len(action) >= 1000:
                utils.run_task_auto_retry(msc.save_data,kwargs={"data":action, **self.data_to})
                action = []
        if len(action):
            utils.run_task_auto_retry(msc.save_data, kwargs={"data": action, **self.data_to})
        # action = []

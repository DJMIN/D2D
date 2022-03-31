#!/usr/bin/env python3
# coding: utf-8


import sys
from . import logger, db, diskcacheofsqlite
from .utils import run_task_auto_retry, task_auto_retry, async_task_auto_retry, get_file_md5, get_md5, log_info
from .utils import set_shell_log, active_log
from .db import *
from .decorators import print_hz_async, print_hz
from .logger import LogFormatter
from .fetch_to_requests import gen_from_clipboard, fetch_to_requests
from . import f2r


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]


def get_realpath_here(file_path=None, with_lineno=False):
    if not file_path:
        sys_g = getattr(sys, '_getframe')
        _line = sys_g().f_back.f_lineno  # 调用此方法的代码的函数
        file_path = sys_g(1).f_code.co_filename  # 哪个文件调了用此方法
    else:
        _line = 1
    return f' "{file_path}:{_line}" ' if with_lineno else os.path.split(os.path.realpath(file_path))[0]

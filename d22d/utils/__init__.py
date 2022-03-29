#!/usr/bin/env python3
# coding: utf-8


import os
import random
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

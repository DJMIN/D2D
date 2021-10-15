#!/usr/bin/env python3
# coding: utf-8


import os
import random
from . import logger, db, diskcacheofsqlite
from .utils import run_task_auto_retry, task_auto_retry, async_task_auto_retry
from .db import *
from .logger import LogFormatter


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]

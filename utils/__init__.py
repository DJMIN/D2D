#!/usr/bin/env python3
# coding: utf-8


import os
import random
from . import logger, db
from .logger import g_log
from .utils import run_task_auto_retry
from .db import *


def get_realpath():
    return os.path.split(os.path.realpath(__file__))[0]

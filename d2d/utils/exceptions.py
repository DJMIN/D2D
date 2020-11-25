#!/usr/bin/env python3
# coding: utf-8
import traceback
from functools import wraps
from .logger import g_log


def exception_wrapper(func):
    @wraps(func)
    def inner(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            func_info = f'{func.__code__.co_filename}:{func.__code__.co_firstlineno}:{func.__name__}()'
            g_log.error('\n[%s]\n%r\n%r' % (func_info, args, kwargs),  e)

    return inner

import asyncio
import time
import six
import wrapt
import logging
import os
import sys
import hashlib
from asyncio import sleep as asleep
from async_timeout import timeout as a_timeout


def get_file_name():
    sys_g = getattr(sys, '_getframe')
    _line = sys_g().f_back.f_lineno  # 调用此方法的代码的函数
    _file_name = sys_g(1).f_code.co_filename
    return _file_name


import threading

get_logger_lock = threading.Lock()


# logging.basicConfig(
#     format='[%(levelname)1.1s][%(asctime)s][%(name)s][%(filename)s:%(lineno)d] %(message)s',
#     level=logging.INFO,
# )
#
#
# def get_logger(name):
#     formatter = logging.Formatter(
#                 fmt='''[%(levelname)1.1s][%(asctime)s][%(name)s] "%(pathname)s:%(lineno)d" %(message)s''')
#     sh = logging.StreamHandler()  # 创建屏幕输出handler
#     sh.setFormatter(formatter)   # 设置handler输出格式
#     sh.setLevel(logging.INFO)   # 设置handler输出等级
#     _log = logging.getLogger(name)   # 创建名为name值的logger
#     _log.setLevel(logging.INFO)
#
#     # 这句话是日志重复的关键
#     # 1.我们想为这个logger设置不一样的输出格式，所以添加了一个屏幕输出handler
#     # 2.没有设置logger的propagate值为False（默认是True），于是依据logging包官方的注释，日志记录会向上传播到他的父节点也就是root logger
#     # 3.当root logger 同时也被添加了屏幕输出handler的情况，日志就会输出第二次
#     # 4.解决方案有两个：一个是去掉root logger的屏幕输出handler，另一个是取消 子logger的 向上传播记录，也就是propagate值设为False
#     _log.addHandler(sh)
#
#     return _log
#
#
# log = get_logger('myselfApp')
# log.info('test')


def get_logger(name=None, formatter=None):
    # os.path.split(__file__)[-1].split(".")[0]

    if not formatter:
        formatter = logging.Formatter(
            fmt='''[%(levelname)1.1s][%(asctime)s][%(name)s] "%(pathname)s:%(lineno)d" %(message)s''')
        formatter_fh = logging.Formatter(fmt='''[%(levelname)1.1s][%(asctime)s][%(pathname)s:%(lineno)d] %(message)s''')
    else:
        formatter_fh = formatter

    if name and name != 'root':
        path = ''
        file_name = name
    elif name == 'root':
        path = ''
        file_name = name
        name = None
    else:
        sys_g = getattr(sys, '_getframe')
        _line = sys_g().f_back.f_lineno  # 调用此方法的代码的函数
        name = sys_g(1).f_code.co_filename
        # path = f'{os.path.dirname(name)}/'
        path = ''
        file_name = os.path.basename(name)
        name = file_name  # 加上这行让name短一点

    path_log = f'{os.getcwd()}/log/{path}'
    if not os.path.exists(path_log):
        os.makedirs(path_log)
    fh = logging.FileHandler(filename=f"{path_log}/{file_name}.log", mode="a", encoding="utf-8")
    fh_err = logging.FileHandler(filename=f"{path_log}/{file_name}_error.log", mode="a", encoding="utf-8")
    sh = logging.StreamHandler()

    fh.setFormatter(formatter_fh)
    fh_err.setFormatter(formatter_fh)
    sh.setFormatter(formatter)

    fh.setLevel(logging.INFO)
    fh_err.setLevel(logging.ERROR)
    sh.setLevel(logging.INFO)

    _log = logging.getLogger(name)
    if name is not None:
        _log.propagate = False

    get_logger_lock.acquire()
    for handler in _log.handlers:
        if isinstance(handler, logging.StreamHandler):
            _log.handlers.remove(handler)
    _log.addHandler(fh)
    _log.addHandler(fh_err)
    _log.addHandler(sh)
    # print(name, _log.handlers)
    get_logger_lock.release()

    _log.setLevel(logging.INFO)

    return _log


formatter_no_path = logging.Formatter(fmt='''[%(levelname)1.1s][%(asctime)s] %(message)s''')
# root_logger = get_logger('root')
# logger_timmer = get_logger('timmer', formatter=formatter_no_path)


def utf8(string):
    """
    Make sure string is utf8 encoded bytes.

    If parameter is a object, object.__str__ will been called before encode as bytes
    """
    if isinstance(string, six.text_type):
        return string.encode('utf8')
    elif isinstance(string, six.binary_type):
        return string
    else:
        return six.text_type(string).encode('utf8')


def get_md5(s):
    md5 = hashlib.md5(utf8(s)).hexdigest()
    return md5


def get_file_md5(file):
    md5 = hashlib.md5()
    f = open(file, 'rb')
    md5.update(f.read())
    f.close()
    return md5.hexdigest()


def async_task_auto_retry(
        warning_d=None,
        error_d=None,
        raise_e=None,
        time_sleep=5,
        max_retry=0,
        timeout=0,
        hard_timeout=0,
        # callback=None,
        # error_callback=None,
        finally_callback=None
):
    if not warning_d:
        warning_d = {}
    if not error_d:
        error_d = {}
    if not raise_e:
        raise_e = []

    """
      :param timeout: 任务超时时间
      :param hard_timeout: 任务超时时间退出
      :param max_retry: 任务最大重试次数
      :param time_sleep: 错误等待时长
      :param func: 任务函数
      :param args: args参数
      :param kwargs: kwargs参数
      :param warning_d: 报警异常类 以及 异常信息 kv字典
      {Exception: "这里是报警信息"}
      {Exception: lambda ex: f'这里是报警信息{ex.x}'}
      :param error_d: 错误异常类 以及 异常信息 kv字典
      {Exception: "这里是错误信息"}
      :param raise_e: 需要抛出异常的异常类
      [Exception]
      :return:
    """

    @wrapt.decorator
    async def wrapper(func, _instance, args, kwargs):
        nonlocal hard_timeout
        nonlocal timeout
        if callable(hard_timeout):
            _hard_timeout = hard_timeout(*args, **kwargs)
        else:
            _hard_timeout = hard_timeout
        if callable(timeout):
            _timeout = timeout(*args, **kwargs)
        else:
            _timeout = timeout

        if not isinstance(args, tuple):
            args = (args,)
        if not kwargs:
            kwargs = {}
        retry = 0
        time_start = time.time()
        func_info = f'{func.__code__.co_filename}:{func.__code__.co_firstlineno}:{func.__name__}()'
        args_info = '[ args: {} ]  |  [ kwargs: {} ]'.format(
            ', '.join(f'{arg}'[:50] for arg in args),
            ', '.join(f'{arg}={v}'[:50] for arg, v in kwargs.items()), )
        # if _instance:
        #     func = partial(func, _instance)
        while True:
            retry += 1
            try:
                logger_atask.debug(
                    f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' + ''
                    # f' {args} {kwargs}'
                )
                try:
                    if _hard_timeout:
                        async with a_timeout(_hard_timeout - int(time.time() - time_start)) as _:
                            result = await func(*args, **kwargs)
                            # print(_.expired)
                    else:
                        result = await func(*args, **kwargs)
                except asyncio.exceptions.TimeoutError:
                    e_t = TimeoutError(f'{func_info}超时 {_hard_timeout}s')
                    setattr(e_t, 'seconds', _hard_timeout)
                    raise e_t
                return result
            except Exception as ex:
                error = ex
                if ex.__class__ in warning_d:
                    ex_msg = warning_d[ex.__class__]
                    if ex_msg is not None:
                        if callable(ex_msg):
                            ex_msg = ex_msg(ex)
                        logger_atask.warning(
                            f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {args_info} {ex_msg}')
                if ex.__class__ in error_d:
                    ex_msg = error_d[ex.__class__]
                    if ex_msg is not None:
                        if callable(ex_msg):
                            ex_msg = ex_msg(ex)
                        logger_atask.error(
                            f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {args_info} {ex_msg}')
                else:
                    import traceback
                    logger_atask.error(
                        f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {args_info} '
                        f' {type(ex)} {str(ex)[:150]}'
                        f'\n{traceback.format_exc()}',
                        # f' {args} {kwargs}',
                    )
                if (raise_e is True) or (ex.__class__ in raise_e):
                    raise ex
            finally:
                if finally_callback is not None:
                    finally_callback(*args, **kwargs)
            if (
                    (max_retry and retry > max_retry) or
                    (_timeout and ((time.time() - time_start) > _timeout))
            ):
                raise error
            await asleep(time_sleep)

    return wrapper


def task_auto_retry(
        warning_d=None,
        error_d=None,
        raise_e=None,
        raise_all=None,
        time_sleep=60,
        max_retry=0,
        timeout=0,
):
    if not warning_d:
        warning_d = {}
    if not error_d:
        error_d = {}
    if not raise_e:
        raise_e = []
    """
      :param timeout: 任务超时时间
      :param max_retry: 任务最大重试次数
      :param time_sleep: 错误等待时长
      :param func: 任务函数
      :param args: args参数
      :param kwargs: kwargs参数
      :param warning_d: 报警异常类 以及 异常信息 kv字典
      {Exception: "这里是报警信息"}
      :param error_d: 错误异常类 以及 异常信息 kv字典
      {Exception: "这里是错误信息"}
      :param raise_e: 需要抛出异常的异常类
      [Exception]
      :return:
    """

    @wrapt.decorator
    def wrapper(func, _instance, args, kwargs):
        if not isinstance(args, tuple):
            args = (args,)
        if not kwargs:
            kwargs = {}
        retry = 0
        time_start = time.time()
        func_info = f'{func.__code__.co_filename}:{func.__code__.co_firstlineno}:{func.__name__}()'
        while True:
            retry += 1
            try:
                logger_task.debug(
                    f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' + ''
                    # f' {args} {kwargs}'
                )
                return func(*args, **kwargs)
            except Exception as ex:
                error = ex
                if ex.__class__ in warning_d:
                    error = ex
                    ex_msg = warning_d[ex.__class__]
                    if ex_msg is not None:
                        logger_task.warning(
                            f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
                if ex.__class__ in error_d:
                    error = ex
                    ex_msg = error_d[ex.__class__]
                    if ex_msg is not None:
                        logger_task.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
                else:
                    logger_task.error(
                        f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s]  {func_info}'
                        f' {type(ex)} {str(ex)[:150]}',
                        # f' {args} {kwargs}',
                    )
                if ex.__class__ in raise_e or raise_all:
                    raise ex
            if (
                    (max_retry and retry > max_retry) or
                    (timeout and ((time.time() - time_start) > timeout))
            ):
                raise error
            time.sleep(time_sleep)

    return wrapper


"""
    This class abstracts the well known startTime - time.time()
    it's a little redundant but it limits calls and allows to temporally pause
    time tracking
"""

import time


class Stopwatch(object):
    """
        defines the class that abstracts time tracking
    """

    def __init__(self):
        """
            initialize variables and control flags
        """
        self.start_time = -1.0
        self.elapsed_time = 0.0
        self.is_stopped = True
        self.is_paused = False
        self.reset_idx = 1
        self.start()

    def start(self):
        """
            sets flags and variables
        """
        if self.is_stopped and not self.is_paused:
            self.is_stopped = False
            self.elapsed_time = 0.0
        elif self.is_paused and not self.is_stopped:
            self.is_paused = False
        else:
            raise RuntimeError('Flags are unproperly set notify code author for him to revise logic')
        self.start_time = time.time()

    def stop(self):
        """
            stops stopwatch elapsed time gets stored but is lost upon restarting
        """

        if self.is_stopped:
            raise RuntimeError('Stopwatch is already stopped')
        self.elapsed_time += time.time() - self.start_time
        self.is_stopped = True
        self.is_paused = False

    def pause(self):
        """
            pauses the stopwatch without loosing elapsed time nor restarting it
        """
        if self.is_stopped:
            raise RuntimeError('Stopwatch is currently stopped, can\'t be paused')
        self.elapsed_time += time.time() - self.start_time
        self.is_paused = True

    def reset(self):
        """
            resets data without stopping the stopwatch itself
        """
        self.start_time = time.time()
        self.elapsed_time = 0
        self.reset_idx += 1

    def get_time(self, name=''):
        """
            returns the time elapsed since the stopwatch got started
            it's redundancy causes a small margin of error, please be weary
        """
        if not self.is_paused and not self.is_stopped:
            self.elapsed_time += time.time() - self.start_time
            self.start_time = time.time()
        print(f'[{self.reset_idx:02d}:{name}]花费了{self.elapsed_time:.5f}s')
        return self.elapsed_time

    def get_time_and_reset(self, name=''):
        """
            returns the time elapsed since the stopwatch got started
            it's redundancy causes a small margin of error, please be weary
        """
        res = self.get_time(name)
        self.reset()
        return res

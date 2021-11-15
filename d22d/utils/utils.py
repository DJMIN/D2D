import hashlib
import ntpath
import random
import string
import sys
import time
import os
import logging

import six
import wrapt
import traceback
import datetime
import shutil
import asyncio.exceptions

from functools import lru_cache
from tornado.log import LogFormatter as _LogFormatter

from sys import platform
from shutil import copy2
from json import JSONEncoder, dumps
from asyncio import sleep
from base64 import b64encode
from async_timeout import timeout as a_timeout

logger = logging.getLogger('d22d.task')


def dump_json(obj):
    if (not obj) or isinstance(obj, type(None)):
        return ''

    return dumps(obj, cls=JSONEncoderWithBytes)


def run_task_auto_retry(
        func, args=(), kwargs=None,
        warning_d=None,
        error_d=None,
        raise_e=None,
        time_sleep=5,
        max_retry=0,
        timeout=0,
):
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
    if not isinstance(args, tuple):
        args = (args,)
    if not kwargs:
        kwargs = {}
    if not warning_d:
        warning_d = {}
    if not error_d:
        error_d = {}
    if not raise_e:
        raise_e = []
    retry = 0
    time_start = time.time()
    func_info = f'{func.__code__.co_filename}:{func.__code__.co_firstlineno}:{func.__name__}()'
    while True:
        retry += 1
        try:
            logger.debug(
                f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' +
                # f' {args} {kwargs}'+
                ''
            )
            res = func(*args, **kwargs)
            logger.info(
                f'[SUC:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' +
                # f' {args} {kwargs}'+
                ''
            )
            return res
        except tuple(warning_d.keys()) as ex:
            error = ex
            ex_msg = warning_d[ex.__class__]
            if ex_msg is not None:
                logger.warning(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
        except tuple(error_d.keys()) as ex:
            error = ex
            ex_msg = error_d[ex.__class__]
            if ex_msg is not None:
                logger.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
            if ex.__class__ in raise_e:
                raise ex
        except tuple({Exception: ""}.keys()) as ex:
            error = ex
            logger.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s]  {func_info}'
                         f' {type(ex)} {str(ex)[:250]:250s}\n{traceback.format_exc()}')
            if ex.__class__ in raise_e:
                raise ex
        if (
                (max_retry and retry > max_retry) or
                (timeout and ((time.time() - time_start) > timeout))
        ):
            raise error
        time.sleep(time_sleep)


def task_auto_retry(
        warning_d=None,
        error_d=None,
        raise_e=None,
        time_sleep=3,
        max_retry=10,
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
                logger.debug(
                    f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' + ''
                    # f' {args} {kwargs}'
                )
                return func(*args, **kwargs)
            except tuple(warning_d.keys()) as ex:
                error = ex
                ex_msg = warning_d[ex.__class__]
                if ex_msg is not None:
                    logger.warning(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
            except tuple(error_d.keys()) as ex:
                error = ex
                ex_msg = error_d[ex.__class__]
                if ex_msg is not None:
                    logger.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
                if ex.__class__ in raise_e:
                    raise ex
            except tuple({Exception: ""}.keys()) as ex:
                error = ex
                logger.error(
                    f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s]  {func_info}'
                    f' {type(ex)} {str(ex)[:250]:250s}\n{traceback.format_exc()}',
                    # f' {args} {kwargs}',
                )
                if ex.__class__ in raise_e:
                    raise ex
            if (
                    (max_retry and retry > max_retry) or
                    (timeout and ((time.time() - time_start) > timeout))
            ):
                raise error
            time.sleep(time_sleep)

    return wrapper


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
        while True:
            retry += 1
            try:
                logger.debug(
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
            except tuple(warning_d.keys()) as ex:
                error = ex
                ex_msg = warning_d[ex.__class__]
                if ex_msg is not None:
                    if callable(ex_msg):
                        ex_msg = ex_msg(ex)
                    logger.warning(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
                if (raise_e is True) or (ex.__class__ in raise_e):
                    raise ex
            except tuple(error_d.keys()) as ex:
                error = ex
                ex_msg = error_d[ex.__class__]
                if ex_msg is not None:
                    if callable(ex_msg):
                        ex_msg = ex_msg(ex)
                    logger.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
                    if (raise_e is True) or (ex.__class__ in raise_e):
                        raise ex
            except tuple({Exception: ""}.keys()) as ex:
                error = ex
                import traceback
                logger.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s]  {func_info}'
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
            await sleep(time_sleep)

    return wrapper


def custom_time(timestamp):
    time_local = time.localtime(timestamp)
    dt = time.strftime("%Y-%m-%d", time_local)
    return dt


class JSONEncoderWithBytes(JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return b64encode(obj).decode('ascii')
        elif hasattr(obj, 'to_json'):
            return obj.to_json()
        elif hasattr(obj, 'to_dict'):
            return dumps(obj.to_dict(), cls=JSONEncoderWithBytes)
        elif hasattr(obj, 'isoformat'):
            # datetime
            return obj.isoformat()
        elif isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        # elif hasattr(obj, '__str__'):
        #     return str(obj)
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        else:
            try:
                return super().default(obj)
            except Exception as ex:
                if hasattr(obj, '__str__'):
                    return str(obj)
                elif hasattr(obj, '__repr__'):
                    return obj.__repr__()
                else:
                    raise ex


def format_error(ex):
    return '[{}] {}\n{}'.format(type(ex), ex, traceback.format_exc())


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
    return str(md5)


def get_file_md5(file):
    md5 = hashlib.md5()
    f = open(file, 'rb')
    md5.update(f.read())
    f.close()
    return str(md5.hexdigest()).lower()


class LogFormatter(_LogFormatter, object):
    def __init__(self, fmt=None, datefmt=None, color=True, *args, **kwargs):
        datefmt = "%Y-%m-%d %H:%M:%S"
        # '[%(threadName)s]' \
        if fmt == 1:
            fmt = '%(color)s[%(asctime)s][%(levelname)1.1s]%(end_color)s%(message)s'
        self.fmt = fmt or '%(color)s[%(asctime)s][%(levelname)1.1s][%(name)s][%(filename)s:%(lineno)d:%(funcName)s()]%(end_color)s%(message)s'

        super(LogFormatter, self).__init__(color=color, fmt=self.fmt, datefmt=datefmt, *args, **kwargs)

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = str(datetime.datetime.now().strftime(datefmt))
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            s = "%s.%03d" % (t, record.msecs)
        return s


logger_info_where = logging.getLogger('info_where')
logger_info_where.setLevel(logging.DEBUG)


def set_shell_log(log, fmt=None):
    # create a file handler
    handler = logging.StreamHandler()
    handler.setLevel(logging.DEBUG)

    # create a logging format
    handler.setFormatter(LogFormatter(fmt=fmt))

    # add the handlers to the logger
    log.addHandler(handler)
    if log.name != 'root':
        log.propagate = False


def log_info(*args, **kwargs):
    # 什么函数调用了此函数
    which_fun_call_this = sys._getframe(1).f_code.co_name  # NOQA
    # 获取被调用函数在被调用时所处代码行数
    line = sys._getframe().f_back.f_lineno
    # 获取被调用函数所在模块文件名
    file_name = sys._getframe(1).f_code.co_filename
    logger_info_where.info(f'"{file_name}:{line}" {which_fun_call_this}()：' + ' '.join(
        [arg.__str__() for arg in args] + [', ', ', '.join(f"{k}={v}" for k, v in kwargs.items())]))


def active_log():
    # set_shell_log(logger)
    set_shell_log(logging.getLogger())
    set_shell_log(logger_info_where, 1)


def activate_debug_logger(level=logging.DEBUG):
    """Global logger used when running from command line."""
    logging.basicConfig(
        format='(%(levelname)s) %(message)s', level=level
    )


def with_cur_lock():
    @wrapt.decorator
    def wrapper(func, instance, args, kwargs):
        lock = instance.cur_lock
        if lock.locked():
            raise IOError(f'{args[0:]} cur locked')
        lock.acquire()
        try:
            res = func(instance.cur, *args, **kwargs)
            return res
        except Exception as ex:
            raise ex
        finally:
            lock.release()

    return wrapper


def gen_pass(val_type='all', val_len=8):
    src_digits = string.digits  # string_数字  '0123456789'
    src_uppercase = string.ascii_uppercase  # string_大写字母 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    src_lowercase = string.ascii_lowercase  # string_小写字母 'abcdefghijklmnopqrstuvwxyz'
    src_special = string.punctuation  # string_特殊字符 '!"#$%&\'()*+,-./:;<=>?@[\\]^_`{|}~'

    if val_type.lower().find('int') == -1:
        # sample从序列中选择n个随机独立的元素，返回列表
        num = random.sample(src_digits, 1)  # 随机取1位数字
        lower = random.sample(src_uppercase, 1)  # 随机取1位小写字母
        upper = random.sample(src_lowercase, 1)  # 随机取1位大写字母
        special = random.sample(src_special, 1)  # 随机取1位大写字母特殊字符
        other = random.sample(string.ascii_letters + string.digits + string.punctuation, val_len - 4)
        # 生成字符串
        # print(num, lower, upper, special, other)
        pwd_list = num + lower + upper + special + other
        # shuffle将一个序列中的元素随机打乱，打乱字符串
        random.shuffle(pwd_list)
        # 列表转字符串
        password_str = ''.join(pwd_list)
        # print(password_str)
    else:
        pwd_list = random.sample(string.digits, val_len)
        random.shuffle(pwd_list)
        # 列表转字符串
        password_str = int(''.join(pwd_list))

    return password_str


def remove_folder(path):
    if os.path.exists(path):
        shutil.rmtree(path)


def makedirs(path):
    folder_name = path.split('\\')[-1].split('/')[-1]
    is_folder = '.' not in folder_name
    path = os.path.realpath(path)
    out_f_path = os.path.dirname(path)
    if is_folder:
        out_f_path = os.path.join(out_f_path, folder_name)
    if not os.path.exists(out_f_path):
        logger.info(f'正在创建新文件夹：{out_f_path}，因为{path}需要')
        os.makedirs(out_f_path)
        

def iter_path(path, exclude=None, include=None, exclude_path=None, include_path=None, return_type=2, open_kwargs=None):
    if exclude is None:
        exclude = []
    if include is None:
        include = []
    if exclude_path is None:
        exclude_path = []
    if include_path is None:
        include_path = []
    if open_kwargs is None:
        open_kwargs = {}
    logging.info(f'开始遍历文件夹：{path}')
    cnt = 0
    cnt_size = 0
    cnt_iter = 0
    for root, fs, fns in os.walk(path):
        for fn in fns:
            cnt += 1
            if include and not any([bool(include_str in fn) for include_str in include]):
                continue
            if exclude and any([bool(exclude_str in fn) for exclude_str in exclude]):
                continue
            f_path = os.path.join(root, fn)
            if include_path and not any([bool(include_str in f_path) for include_str in include_path]):
                continue
            if exclude_path and any([bool(exclude_str in f_path) for exclude_str in exclude_path]):
                continue
            cnt_size += os.path.getsize(f_path)
            cnt_iter += 1
            logging.debug(f'遍历到文件：{f_path} [已处理{cnt_iter}|返回{cnt_iter}个：{cnt_size / 1024 / 1024:.3f}MB]')
            if return_type == 0:
                yield cnt, root, *os.path.splitext(fn)
            elif return_type == 1:
                yield cnt, root, fn
            elif return_type == 2:
                yield cnt, f_path
            elif return_type == 3:
                yield f_path
            elif return_type == 4:
                yield cnt, open(f_path, 'rb', **open_kwargs)
            elif return_type == 5:
                yield open(f_path, 'rb', **open_kwargs)
    logging.info(f'遍历文件结束 [{cnt_size / 1024 / 1024:.3f}MB] [已处理{cnt_iter}|返回{cnt_iter}个]：{path}')


def path_without_leaf(path):
    result = ""
    try:
        head, tail = ntpath.split(path)
        result = head
    except Exception as inst:
        logger.exception(f"path_without_leaf Error - {path}: {inst}")
    return result


# return the name of the file + extension
def path_leaf(path):
    result = ""
    try:
        head, tail = ntpath.split(path)
        result = tail or ntpath.basename(head)
    except Exception as inst:
        logger.exception(f"Path_leaf Error - {path}: {inst}")
    return result


def create_temporary_copy(path, temp_dir, file_name):
    if platform == "win32":
        file_to_check = temp_dir + '\\' + file_name
    else:
        file_to_check = temp_dir + '/' + file_name
    # if the file already exists, we will delete it
    exists = os.path.isfile(file_to_check)
    if exists:
        os.chmod(file_to_check, 777)
        os.remove(file_to_check)
    temp_path = os.path.join(temp_dir, file_name)
    copy2(path, temp_path)
    return temp_path


def time_stamp():
    """returns a formatted current time/date"""
    import time
    return str(time.strftime("%Y_%m_%d_%H_%M_%S"))

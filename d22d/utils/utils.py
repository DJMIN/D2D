import random
import string
import time
import logging
import wrapt
import traceback
import datetime
import asyncio.exceptions
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

if __name__ == '__main__':
    run_task_auto_retry(run_task_auto_retry)

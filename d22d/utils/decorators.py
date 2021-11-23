# coding=utf-8
import base64
import copy
import random
import uuid
import collections
import wrapt
from flask import request as flask_request
# noinspection PyUnresolvedReferences
from contextlib import contextmanager
import functools
import json
import os
import sys
import threading
import time
import traceback
import logging
import unittest
from functools import wraps
# noinspection PyUnresolvedReferences
import pysnooper
from tomorrow3 import threads as tomorrow_threads

# noinspection PyUnresolvedReferences

LogManager = logging.getLogger
handle_exception_log = logging.getLogger('function_error')
run_times_log = logging.getLogger('run_many_times')


class CustomException(Exception):
    def __init__(self, err=''):
        err0 = 'fatal exception\n'
        Exception.__init__(self, err0 + err)


def run_many_times(times=1):
    """把函数运行times次的装饰器
    :param times:运行次数
    没有捕获错误，出错误就中断运行，可以配合handle_exception装饰器不管是否错误都运行n次。
    """

    def _run_many_times(func):
        @wraps(func)
        def __run_many_times(*args, **kwargs):
            for i in range(times):
                run_times_log.debug('* ' * 50 + '当前是第 {} 次运行[ {} ]函数'.format(i + 1, func.__name__))
                func(*args, **kwargs)

        return __run_many_times

    return _run_many_times


# noinspection PyIncorrectDocstring
def handle_exception(retry_times=0, error_detail_level=0, is_throw_error=False, time_sleep=0):
    """捕获函数错误的装饰器,重试并打印日志
    :param retry_times : 重试次数
    :param error_detail_level :为0打印exception提示，为1打印3层深度的错误堆栈，为2打印所有深度层次的错误堆栈
    :param is_throw_error : 在达到最大次数时候是否重新抛出错误
    :type error_detail_level: int
    """

    if error_detail_level not in [0, 1, 2]:
        raise Exception('error_detail_level参数必须设置为0 、1 、2')

    def _handle_exception(func):
        @wraps(func)
        def __handle_exception(*args, **keyargs):
            for i in range(0, retry_times + 1):
                try:
                    result = func(*args, **keyargs)
                    if i:
                        handle_exception_log.debug(
                            u'%s\n调用成功，调用方法--> [  %s  ] 第  %s  次重试成功' % ('# ' * 40, func.__name__, i))
                    return result

                except Exception as e:
                    error_info = ''
                    if error_detail_level == 0:
                        error_info = '错误类型是：' + str(e.__class__) + '  ' + str(e)
                    elif error_detail_level == 1:
                        error_info = '错误类型是：' + str(e.__class__) + '  ' + traceback.format_exc(limit=3)
                    elif error_detail_level == 2:
                        error_info = '错误类型是：' + str(e.__class__) + '  ' + traceback.format_exc()

                    handle_exception_log.exception(
                        u'%s\n记录错误日志，调用方法--> [  %s  ] 第  %s  次错误重试， %s\n' % ('- ' * 40, func.__name__, i, error_info))
                    if i == retry_times and is_throw_error:  # 达到最大错误次数后，重新抛出错误
                        raise e
                time.sleep(time_sleep)

        return __handle_exception

    return _handle_exception


def keep_circulating(time_sleep=0.001, exit_if_function_run_sucsess=False, is_display_detail_exception=True, block=True,
                     daemon=False):
    """间隔一段时间，一直循环运行某个方法的装饰器
    :param time_sleep :循环的间隔时间
    :param exit_if_function_run_sucsess :如果成功了就退出循环
    :param is_display_detail_exception
    :param block :是否阻塞主主线程，False时候开启一个新的线程运行while 1。
    """
    if not hasattr(keep_circulating, 'keep_circulating_log'):
        keep_circulating.log = logging.getLogger('keep_circulating')

    def _keep_circulating(func):
        @wraps(func)
        def __keep_circulating(*args, **kwargs):

            # noinspection PyBroadException
            def ___keep_circulating():
                while 1:
                    try:
                        result = func(*args, **kwargs)
                        if exit_if_function_run_sucsess:
                            return result
                    except Exception as e:
                        msg = func.__name__ + '   运行出错\n ' + traceback.format_exc(
                            limit=10) if is_display_detail_exception else str(e)
                        keep_circulating.log.error(msg)
                    finally:
                        time.sleep(time_sleep)

            if block:
                return ___keep_circulating()
            else:
                threading.Thread(target=___keep_circulating, daemon=daemon).start()

        return __keep_circulating

    return _keep_circulating


def synchronized(func):
    """线程锁装饰器，可以加在单例模式上"""
    func.__lock__ = threading.Lock()

    @wraps(func)
    def lock_func(*args, **kwargs):
        with func.__lock__:
            return func(*args, **kwargs)

    return lock_func


def singleton(cls):
    """
    单例模式装饰器,新加入线程锁，更牢固的单例模式，主要解决多线程如100线程同时实例化情况下可能会出现三例四例的情况,实测。
    """
    _instance = {}
    singleton.__lock = threading.Lock()

    @wraps(cls)
    def _singleton(*args, **kwargs):
        with singleton.__lock:
            if cls not in _instance:
                _instance[cls] = cls(*args, **kwargs)
            return _instance[cls]

    return _singleton


def flyweight(cls):
    _instance = {}

    def _make_arguments_to_key(args, kwds):
        key = args
        if kwds:
            sorted_items = sorted(kwds.items())
            for item in sorted_items:
                key += item
        return key

    @synchronized
    @wraps(cls)
    def _flyweight(*args, **kwargs):
        # locals_copy = copy.deepcopy(locals())
        # import inspect
        # nb_print(inspect.getfullargspec(cls.__init__))
        # nb_print(cls.__init__.__defaults__)  # 使用__code__#总参数个数
        #
        # nb_print(cls.__init__.__code__.co_argcount)  # 总参数名
        #
        # nb_print(cls.__init__.__code__.co_varnames)
        #
        #
        # nb_print(locals_copy)
        # cache_param_value_list = [locals_copy.get(param, None) for param in cache_params]

        cache_key = f'{cls}_{_make_arguments_to_key(args, kwargs)}'
        # nb_print(cache_key)
        if cache_key not in _instance:
            _instance[cache_key] = cls(*args, **kwargs)
        return _instance[cache_key]

    return _flyweight


def timer(func):
    """计时器装饰器，只能用来计算函数运行时间"""
    if not hasattr(timer, 'log'):
        timer.log = LogManager(f'timer_{func.__name__}')

    @wraps(func)
    def _timer(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        t_spend = round(t2 - t1, 2)
        timer.log.debug('执行[ {} ]方法用时 {} 秒'.format(func.__name__, t_spend))
        return result

    return _timer


# noinspection PyProtectedMember
class TimerContextManager(object):
    """
    用上下文管理器计时，可对代码片段计时
    """

    def __init__(self, is_print_log=True):
        self._is_print_log = is_print_log
        self.t_spend = None
        self._line = None
        self._file_name = None
        self.time_start = None

    def __enter__(self):
        self._line = sys._getframe().f_back.f_lineno  # 调用此方法的代码的函数
        self._file_name = sys._getframe(1).f_code.co_filename  # 哪个文件调了用此方法
        self.time_start = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.t_spend = time.time() - self.time_start
        if self._is_print_log:
            logging.debug(f'对下面代码片段进行计时:  \n执行"{self._file_name}:{self._line}" 用时 {round(self.t_spend, 2)} 秒')


class RedisDistributedLockContextManager(object):
    """
    分布式redis锁自动管理.
    """

    def __init__(self, redis_client, redis_lock_key, expire_seconds=30):
        self.redis_client = redis_client
        self.redis_lock_key = redis_lock_key
        self._expire_seconds = expire_seconds
        self.identifier = str(uuid.uuid4())
        self.has_aquire_lock = False

    def __enter__(self):
        self._line = sys._getframe().f_back.f_lineno  # 调用此方法的代码的函数
        self._file_name = sys._getframe(1).f_code.co_filename  # 哪个文件调了用此方法
        self.redis_client.set(self.redis_lock_key, value=self.identifier, ex=self._expire_seconds, nx=True)
        identifier_in_redis = self.redis_client.get(self.redis_lock_key)
        if identifier_in_redis and identifier_in_redis.decode() == self.identifier:
            self.has_aquire_lock = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.has_aquire_lock:
            self.redis_client.delete(self.redis_lock_key)
        if self.has_aquire_lock:
            log_msg = f'\n"{self._file_name}:{self._line}" 这行代码获得了redis锁 {self.redis_lock_key}'
            logging.info(log_msg)
        else:
            log_msg = f'\n"{self._file_name}:{self._line}" 这行代码没有获得redis锁 {self.redis_lock_key}'
            logging.warning(log_msg)


"""
@contextmanager
        def some_generator(<arguments>):
            <setup>
            try:
                yield <value>
            finally:
                <cleanup>
"""


class ExceptionContextManager:
    """
    用上下文管理器捕获异常，可对代码片段进行错误捕捉，比装饰器更细腻
    """

    def __init__(self, logger_name='ExceptionContextManager', verbose=100, donot_raise__exception=True, ):
        """
        :param verbose: 打印错误的深度,对应traceback对象的limit，为正整数
        :param donot_raise__exception:是否不重新抛出错误，为Fasle则抛出，为True则不抛出
        """
        self.logger = LogManager(logger_name)
        self._verbose = verbose
        self._donot_raise__exception = donot_raise__exception

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # print(exc_val)
        # print(traceback.format_exc())
        exc_str = str(exc_type) + '  :  ' + str(exc_val)
        exc_str_color = '\033[0;30;45m%s\033[0m' % exc_str
        if self._donot_raise__exception:
            if exc_tb is not None:
                self.logger.error('\n'.join(traceback.format_tb(exc_tb)[:self._verbose]) + exc_str_color)
        return self._donot_raise__exception  # __exit__方法必须retuen True才会不重新抛出错误


def where_is_it_called(func):
    """一个装饰器，被装饰的函数，如果被调用，将记录一条日志,记录函数被什么文件的哪一行代码所调用"""
    if not hasattr(where_is_it_called, 'log'):
        where_is_it_called.log = LogManager('where_is_it_called')

    # noinspection PyProtectedMember
    @wraps(func)
    def _where_is_it_called(*args, **kwargs):
        # 获取被调用函数名称
        # func_name = sys._getframe().f_code.co_name
        func_name = func.__name__
        # 什么函数调用了此函数
        which_fun_call_this = sys._getframe(1).f_code.co_name  # NOQA

        # 获取被调用函数在被调用时所处代码行数
        line = sys._getframe().f_back.f_lineno

        # 获取被调用函数所在模块文件名
        file_name = sys._getframe(1).f_code.co_filename

        # noinspection PyPep8
        where_is_it_called.log.debug(
            f'文件[{func.__code__.co_filename}]的第[{func.__code__.co_firstlineno}]行即模块 [{func.__module__}] 中的方法 [{func_name}] 正在被文件 [{file_name}] 中的'
            f'方法 [{which_fun_call_this}] 中的第 [{line}] 行处调用，传入的参数为[{args},{kwargs}]')
        try:
            t0 = time.time()
            result = func(*args, **kwargs)
            result_raw = result
            t_spend = round(time.time() - t0, 2)
            if isinstance(result, dict):
                result = json.dumps(result)
            if len(str(result)) > 200:
                result = str(result)[0:200] + '  。。。。。。  '
            where_is_it_called.log.debug('执行函数[{}]消耗的时间是{}秒，返回的结果是 --> '.format(func_name, t_spend) + str(result))
            return result_raw
        except Exception as e:
            where_is_it_called.log.error(
                f'发生错误，文件[{func.__code__.co_filename}]的第[{func.__code__.co_firstlineno}]行即模块 [{func.__module__}] 中的方法 [{func_name}] 正在被文件 [{file_name}] 中的'
                f'方法 [{which_fun_call_this}] 中的第 [{line}] 行处调用，传入的参数为[{args},{kwargs}]'
                f'错误[{type(e)}] {e} {traceback.format_exc()}')
            # where_is_it_called.log.exception(e)
            raise e

    return _where_is_it_called


# noinspection PyPep8Naming
class cached_class_property(object):
    """类属性缓存装饰器"""

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self
        value = self.func(obj)
        setattr(cls, self.func.__name__, value)
        return value


# noinspection PyPep8Naming
class cached_property(object):
    """实例属性缓存装饰器"""

    def __init__(self, func):
        self.func = func

    def __get__(self, obj, cls):
        print(obj, cls)
        if obj is None:
            return self
        value = obj.__dict__[self.func.__name__] = self.func(obj)
        return value


def cached_method_result(fun):
    """方法的结果装饰器,不接受self以外的多余参数，主要用于那些属性类的property方法属性上，配合property装饰器，主要是在pycahrm自动补全上比上面的装饰器好"""

    @wraps(fun)
    def inner(self):
        if not hasattr(fun, 'result'):
            result = fun(self)
            fun.result = result
            fun_name = fun.__name__
            setattr(self.__class__, fun_name, result)
            setattr(self, fun_name, result)
            return result
        else:
            return fun.result

    return inner


def cached_method_result_for_instance(fun):
    """方法的结果装饰器,不接受self以外的多余参数，主要用于那些属性类的property方法属性上"""

    @wraps(fun)
    def inner(self):
        if not hasattr(fun, 'result'):
            result = fun(self)
            fun.result = result
            fun_name = fun.__name__
            setattr(self, fun_name, result)
            return result
        else:
            return fun.result

    return inner


class FunctionResultCacher:
    logger = LogManager('FunctionResultChche')
    func_result_dict = {}
    """
    {
        (f1,(1,2,3,4)):(10,1532066199.739),
        (f2,(5,6,7,8)):(26,1532066211.645),
    }
    """

    @classmethod
    def cached_function_result_for_a_time(cls, cache_time: float):
        """
        函数的结果缓存一段时间装饰器,不要装饰在返回结果是超大字符串或者其他占用大内存的数据结构上的函数上面。
        :param cache_time :缓存的时间
        :type cache_time : float
        """

        def _cached_function_result_for_a_time(fun):

            @wraps(fun)
            def __cached_function_result_for_a_time(*args, **kwargs):
                # print(cls.func_result_dict)
                # if len(cls.func_result_dict) > 1024:
                if sys.getsizeof(cls.func_result_dict) > 100 * 1000 * 1000:
                    cls.func_result_dict.clear()

                key = cls._make_arguments_to_key(args, kwargs)
                if (fun, key) in cls.func_result_dict and time.time() - cls.func_result_dict[(fun, key)][
                    1] < cache_time:
                    return cls.func_result_dict[(fun, key)][0]
                else:
                    cls.logger.debug('函数 [{}] 此次不能使用缓存'.format(fun.__name__))
                    result = fun(*args, **kwargs)
                    cls.func_result_dict[(fun, key)] = (result, time.time())
                    return result

            return __cached_function_result_for_a_time

        return _cached_function_result_for_a_time

    @staticmethod
    def _make_arguments_to_key(args, kwds):
        key = args
        if kwds:
            sorted_items = sorted(kwds.items())
            for item in sorted_items:
                key += item
        return key  # 元祖可以相加。


# noinspection PyUnusedLocal
class __KThread(threading.Thread):
    def __init__(self, *args, **kwargs):
        threading.Thread.__init__(self, *args, **kwargs)
        self.killed = False
        self.__run_backup = None

    # noinspection PyAttributeOutsideInit
    def start(self):
        """Start the thread."""
        self.__run_backup = self.run
        self.run = self.__run  # Force the Thread to install our trace.
        threading.Thread.start(self)

    def __run(self):
        """Hacked run function, which installs the trace."""
        sys.settrace(self.globaltrace)
        self.__run_backup()
        self.run = self.__run_backup

    def globaltrace(self, frame, why, arg):
        if why == 'call':
            return self.localtrace
        return None

    def localtrace(self, frame, why, arg):
        if self.killed:
            if why == 'line':
                raise SystemExit()
        return self.localtrace

    def kill(self):
        self.killed = True


# noinspection PyPep8Naming
class TIMEOUT_EXCEPTION(Exception):
    """function run timeout"""
    pass


def timeout(seconds):
    """超时装饰器，指定超时时间

    若被装饰的方法在指定的时间内未返回，则抛出Timeout异常"""

    def timeout_decorator(func):

        def _new_func(oldfunc, result, oldfunc_args, oldfunc_kwargs):
            result.append(oldfunc(*oldfunc_args, **oldfunc_kwargs))

        def _(*args, **kwargs):
            result = []
            new_kwargs = {
                'oldfunc': func,
                'result': result,
                'oldfunc_args': args,
                'oldfunc_kwargs': kwargs
            }

            thd = __KThread(target=_new_func, args=(), kwargs=new_kwargs)
            thd.start()
            thd.join(seconds)
            alive = thd.isAlive()
            thd.kill()  # kill the child thread

            if alive:
                # raise TIMEOUT_EXCEPTION('function run too long, timeout %d seconds.' % seconds)
                raise TIMEOUT_EXCEPTION(f'{func.__name__}运行时间超过{seconds}秒')
            else:
                if result:
                    return result[0]
                return result

        _.__name__ = func.__name__
        _.__doc__ = func.__doc__
        return _

    return timeout_decorator


def browse_history(logger, request, route, method, express=''):
    """
    用户浏览记录装饰器
    :param logger:
    :param request:
    :param route:请求路由
    :param method: GET or POST
    :param express: 代码表达式,这里动态执行代码
    :return:
    """

    def decorate(func):
        # noinspection PyPep8Naming
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            bhv_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            token = request.headers.get('x-access-token', '')
            if token:
                # noinspection PyBroadException
                try:
                    # noinspection PyUnresolvedReferences
                    userId = jwt.decode(token, token_secret_key)['user']['userId']
                except Exception:
                    userId = ''
            else:
                userId = ''

            if method == 'POST':
                request_params = json.loads(request.get_data().decode("utf-8"))

            else:
                request_params = request.args

            back = func(*args, **kwargs)
            if express:  # 为了兼容不同场景,这里只能动态写代码插入
                exec(express)
            else:
                userlog = {'user_id': userId, 'bhv_time': bhv_time, 'location': '', 'action_id': route,
                           "action": request_params}
                logger.info(json.dumps(userlog))
            return back

        return wrapper

    return decorate


flask_error_logger = LogManager('flask_error')


def api_return_deco(v):
    """
    对flask的返回 加一个固定的状态码。在测试环境即使是非debug，直接在错误信息中返回错误堆栈。在生产环境使用随机四位字母 加 错误信息的base64作为错误信息。
    :param v:视图函数
    :return:
    """
    env = 'test'

    @wraps(v)
    def _api_return_deco(*args, **kwargs):
        # noinspection PyBroadException
        try:
            data = v(*args, **kwargs)
            return json.dumps({
                "code": 200,
                "data": data,
                "message": "SUCCESS"}, ensure_ascii=False)
        except Exception as e:
            except_str0 = f'请求路径：{flask_request.path}  请求参数：{json.dumps(flask_request.values.to_dict())} ,出错了 {type(e)} {e} {traceback.format_exc()}'.replace(
                '\n', '<br>')
            flask_error_logger.exception(except_str0)
            exception_str_encode = base64.b64encode(except_str0.encode()).decode().replace('=', '').strip()
            message = except_str0 if env == 'test' else f'''{"".join(random.sample("abcdefghijklmnopqrstABCDEFGHIJKLMNOPQRST", 4))}{exception_str_encode}'''
            return json.dumps({
                "code": 500,
                "data": None,
                "message": message}, ensure_ascii=False)

    return _api_return_deco


logger_timmer = logging.getLogger('timmer')


# 定义一个函数用来统计传入函数的运行时间
def timmer():
    # 传入的参数是一个函数
    @wrapt.decorator
    def deco(func, instance, args, kwargs):
        # 本应传入运行函数的各种参数
        # print('\n函数：{_funcname_}开始运行：'.format(_funcname_=func.__name__))
        start_time = time.time()
        # 调用代运行的函数，并将各种原本的参数传入
        # if instance:
        #     res = func(instance, *args, **kwargs)
        # else:
        sys_g = getattr(sys, '_getframe')
        _line = sys_g().f_back.f_lineno  # 调用此方法的代码的函数
        _file_name = sys_g(1).f_code.co_filename  # 哪个文件调了用此方法
        func_name = f'"{_file_name}:{_line}":{func.__name__}()' \
                    f''
        logger_timmer.debug('开始执行函数: [{_funcname_:20s}]'.format(_funcname_=func_name))
        res = func(*args, **kwargs)
        end_time = time.time()
        resl = '[{_long_}耗时][{_time_:.4f}s] 函数:[{_func_:20s}] 入参:{_kwargs_:50s}{_args_:50s} 结果为: {_res_:50s}'.format(
            _long_='长' if end_time - start_time > 1 else '', _func_=func_name, _time_=(end_time - start_time),
            _args_=str(args), _kwargs_=str(kwargs), _res_=str(res))
        try:
            logger_timmer.info(resl)
        except Exception as e:
            print(e, resl)
        # 返回值为函数的运行结果
        return res

    # 返回值为函数
    return deco


def timmer_async():
    # 传入的参数是一个函数
    @wrapt.decorator
    async def deco(func, instance, args, kwargs):
        # 本应传入运行函数的各种参数
        # print('\n函数：{_funcname_}开始运行：'.format(_funcname_=func.__name__))
        start_time = time.time()
        # 调用代运行的函数，并将各种原本的参数传入
        # if instance:
        #     res = func(instance, *args, **kwargs)
        # else:
        sys_g = getattr(sys, '_getframe')
        _line = sys_g().f_back.f_lineno  # 调用此方法的代码的函数
        _file_name = sys_g(1).f_code.co_filename  # 哪个文件调了用此方法
        func_name = f'"{_file_name}:{_line}":{func.__name__}()'
        logger_timmer.debug('开始执行函数: [{_funcname_:20s}]'.format(_funcname_=func_name))
        res = await func(*args, **kwargs)
        end_time = time.time()
        logger_timmer.info(
            '[{_long_}耗时][{_time_:06.4f}s] 函数:[{_func_:20s}] 入参:{_kwargs_}{_args_} 结果为: {_res_:50s}'.format(
                _long_='长' if end_time - start_time > 1 else '', _func_=func_name, _time_=(end_time - start_time),
                _args_=str(args), _kwargs_=str(kwargs), _res_=str(res)))
        # 返回值为函数的运行结果
        return res

    # 返回值为函数
    return deco


start_time = time.time()
cnt_hz = collections.defaultdict(int)
last_print_hz = collections.defaultdict(int)
max_print_hz = collections.defaultdict(int)


def print_hz(
        name='',
        per_cnt_print=2000,
        per_second_print=1,
        print_func=logger_timmer.info
):
    """
    统计函数每秒执行频次, 装饰器本身耗时约0.000001秒/次, （0.001毫秒 1微秒 1000纳秒）/次，100W次/s
    :param  name 函数名留空为自动读取函数名
    :param  per_cnt_print 每2000次执行成功打印一次
    :param  per_second_print 每1秒打印一次
    :param  print_func 打印函数
    """
    global start_time
    global last_print_hz
    global cnt_hz

    # 传入的参数是一个函数
    @wrapt.decorator
    def deco(func, instance, args, kwargs):
        # 本应传入运行函数的各种参数
        # print('\n函数：{_funcname_}开始运行：'.format(_funcname_=func.__name__))
        # 调用代运行的函数，并将各种原本的参数传入
        # if instance:
        #     res = func(instance, *args, **kwargs)
        # else:
        res = func(*args, **kwargs)
        cnt = cnt_hz[name] + 1
        cnt_hz[name] = cnt
        time_now = time.time()
        if (last_print_hz[name] + per_second_print) < time_now or not cnt % per_cnt_print:
            last_print_hz[name] = int(time_now)
            use_time = time_now - start_time
            hz_now = cnt / use_time
            bb = max_print_hz[name] = max([max_print_hz[name], hz_now])
            print_func('[PID:{_now_pid_:09}] {_time_:.4f}秒 执行 {_cnt_} 次函数:[{_func_:20s}]，'
                       '平均/峰值 [{_cnt_per_:.2f}/{_max_cnt_:.2f}] 次/秒'.format(
                            _now_pid_=os.getpid(), _func_=name, _time_=use_time,
                            _cnt_=cnt, _cnt_per_=hz_now, _max_cnt_=bb))
        # 返回值为函数的运行结果
        return res

    # 返回值为函数
    return deco


def print_hz_async(
        name='',
        per_cnt_print=2000,
        per_second_print=1,
        print_func=logger_timmer.info
):
    """
    统计函数每秒执行频次, 装饰器本身耗时约0.000001秒/次, （0.001毫秒 1微秒 1000纳秒）/次，100W次/s
    :param  name 函数名留空为自动读取函数名
    :param  per_cnt_print 每2000次执行成功打印一次
    :param  per_second_print 每1秒打印一次
    :param  print_func 打印函数
    """
    global start_time
    global last_print_hz
    global cnt_hz
    global now_pid

    # 传入的参数是一个函数
    @wrapt.decorator
    async def deco(func, instance, args, kwargs):
        # 本应传入运行函数的各种参数
        # print('\n函数：{_funcname_}开始运行：'.format(_funcname_=func.__name__))
        # 调用代运行的函数，并将各种原本的参数传入
        # if instance:
        #     res = func(instance, *args, **kwargs)
        # else:
        res = await func(*args, **kwargs)
        cnt = cnt_hz[name] + 1
        cnt_hz[name] = cnt
        time_now = time.time()
        if (last_print_hz[name] + per_second_print) < time_now or not cnt % per_cnt_print:
            last_print_hz[name] = int(time_now)
            use_time = time_now - start_time
            hz_now = cnt / use_time
            bb = max_print_hz[name] = max([max_print_hz[name], hz_now])
            print_func('[PID:{_now_pid_:09}] {_time_:.4f}秒 执行 {_cnt_} 次函数:[{_func_:20s}]，'
                       '平均/峰值 [{_cnt_per_:.2f}/{_max_cnt_:.2f}] 次/秒'.format(
                            _now_pid_=now_pid, _func_=name, _time_=use_time,
                            _cnt_=cnt, _cnt_per_=hz_now, _max_cnt_=bb))
        # 返回值为函数的运行结果
        return res

    # 返回值为函数
    return deco

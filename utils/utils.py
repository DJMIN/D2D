import time
from json import JSONEncoder, dumps
from base64 import b64encode
from .logger import g_log


class JSONEncoderWithBytes(JSONEncoder):
    def default(self, o):
        if isinstance(o, bytes):
            return b64encode(o).decode('ascii')

        elif hasattr(o, 'to_dict'):
            return o.to_dict()

        elif hasattr(o, 'isoformat'):
            # datetime
            return o.isoformat()

        elif hasattr(o, '__str__'):
            return str(o)

        return super().default(o)


def dump_json(obj):
    '''
    Dump `obj` into a JSON string.
    '''
    if obj == None:
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
            g_log.debug(
                f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info}' +
                f' {args} {kwargs}')
            return func(*args, **kwargs)
        except tuple(warning_d.keys()) as ex:
            error = ex
            ex_msg = warning_d[ex.__class__]
            if ex_msg is not None:
                g_log.warning(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
        except tuple(error_d.keys()) as ex:
            error = ex
            ex_msg = warning_d[ex.__class__]
            if ex_msg is not None:
                g_log.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s] {func_info} {ex_msg}')
            if ex.__class__ in raise_e:
                raise ex
        except tuple({Exception: ""}.keys()) as ex:
            error = ex
            g_log.error(f'[RetryS:{retry:04d}:{time.time() - time_start:.2f}s]  {func_info}', ex)
            if ex.__class__ in raise_e:
                raise ex
        if (
                (max_retry and retry > max_retry) or
                (timeout and ((time.time() - time_start) > timeout))
        ):
            raise error
        time.sleep(time_sleep)


if __name__ == '__main__':
    run_task_auto_retry(run_task_auto_retry)
